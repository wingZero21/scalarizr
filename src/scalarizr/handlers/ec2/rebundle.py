'''
Created on Mar 11, 2010
@author: marat
'''

import scalarizr
from scalarizr.bus import bus
from scalarizr.handlers import Handler, async, HandlerError
from scalarizr.messaging import Messages, Queues
from scalarizr.util import system, disttool, cryptotool, fstool, filetool,\
	wait_until, get_free_devname
from scalarizr.util import software
from scalarizr.platform.ec2 import s3tool, ebstool

from subprocess import Popen
from M2Crypto import X509, EVP, Rand, RSA
from binascii import hexlify
from xml.dom.minidom import Document
from datetime import datetime
import logging, time, os, re, shutil, glob

import boto
from boto.resultset import ResultSet
from boto.ec2.blockdevicemapping import EBSBlockDeviceType, BlockDeviceMapping
from boto.exception import BotoServerError
from boto.ec2.volume import Volume
from scalarizr.util.filetool import read_file

if boto.Version == '1.9b':
	# Workaround for http://code.google.com/p/boto/issues/detail?id=310
	# `VirtualName` support in block device mapping
	def build_list_params(self, params, prefix=''):
		i = 1
		for dev_name in self:
			pre = '%sBlockDeviceMapping.%d' % (prefix, i)
			params['%s.DeviceName' % pre] = dev_name
			block_dev = self[dev_name]
			if isinstance(block_dev, EBSBlockDeviceType):
				if block_dev.snapshot_id:
					params['%s.Ebs.SnapshotId' % pre] = block_dev.snapshot_id
				if block_dev.size:
					params['%s.Ebs.VolumeSize' % pre] = block_dev.size
				if block_dev.delete_on_termination:
					params['%s.Ebs.DeleteOnTermination' % pre] = 'true'
				else:
					params['%s.Ebs.DeleteOnTermination' % pre] = 'false'
			else:
				params['%s.VirtualName' % pre] = block_dev
			i += 1		

	BlockDeviceMapping.build_list_params = build_list_params


# Workaround for python bug #5853
# @see http://bugs.python.org/issue5853
# @see http://groups.google.com/group/smug-dev/browse_thread/thread/47e7833edb9efbf9?pli=1
import mimetypes
mimetypes.init()

class StopRebundle(BaseException): 
	'''
	Special exception for raising from 'before_rebundle' event listener to stop rebundle process 
	'''
	pass

def get_handlers ():
	return [Ec2RebundleHandler()]


BUNDLER_NAME = "scalarizr"
BUNDLER_VERSION = scalarizr.__version__
BUNDLER_RELEASE = "672"

DIGEST_ALGO = "sha1"
CRYPTO_ALGO = "aes-128-cbc"

WALL_MESSAGE = 'Server is going to rebundle'

MOTD = '''Scalr image 
%(dist_name)s %(dist_version)s %(bits)d-bit
Role: %(role_name)s
Bundled: %(bundle_date)s
'''

class Ec2RebundleHandler(Handler):
	_logger = None
	_ebs_strategy_cls = None
	_instance_store_strategy_cls = None
	
	def __init__(self, ebs_strategy_cls=None, instance_store_strategy_cls=None):
		self._log_hdlr = RebundleLogHandler()		
		self._logger = logging.getLogger(__name__)
		
		self._ebs_strategy_cls = ebs_strategy_cls or RebundleEbsStrategy
		self._instance_store_strategy_cls = instance_store_strategy_cls or RebundleInstanceStoreStrategy
		
		bus.define_events(
			# Fires before rebundle starts
			"before_rebundle", 
			
			# Fires after rebundle complete
			# @param param: 
			"rebundle", 
			
			# Fires on rebundle error
			# @param role_name
			"rebundle_error",
			
			# Fires on bundled volume cleanup. Usefull to remove password files, user activity, logs
			# @param image_mpoint 
			"rebundle_cleanup_image"
		)

	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE
	

	def on_Rebundle(self, message):
		'''
		@param message: 
			Rebundle message
		@param message.role_name: *
			Rebundled role name
		@param message.excludes: * 
			Directories exclude list. Ex: [/home/mysite, /root/.mysetting]
		@param message.volume_size:  
			New size for EBS-root device. 
			By default current EBS-root size will be used (15G in most popular AMIs)	
		@param message.volume_id  
			EBS volume for root device copy.
		'''
		strategy = None
		try:
			self._log_hdlr.bundle_task_id = message.bundle_task_id
			self._logger.addHandler(self._log_hdlr)			
			
			# Obtain role name
			role_name = message.role_name.encode("ascii")
			image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")

			# Create exclude directories list
			excludes = message.excludes.encode("ascii").split(":") \
					if message.body.has_key("excludes") and message.excludes else []

			# Take rebundle strategy
			pl = bus.platform 
			ec2_conn = pl.new_ec2_conn()
			instance = ec2_conn.get_all_instances([pl.get_instance_id()])[0].instances[0]
			if instance.root_device_name:
				# EBS-root device instance
				if 'volume_size' in message.body:
					volume_size = message.volume_size
				else:
					root_bdt = instance.block_device_mapping[instance.root_device_name]
					volume_size = ec2_conn.get_all_volumes([root_bdt.volume_id])[0].size
				
				strategy = self._ebs_strategy_cls(
					role_name, image_name, excludes,
					devname=get_free_devname(), 
					volume_size=volume_size, 
					volume_id=message.body.get('volume_id')
				)
			else:
				# Old-style instance-store
				sda1_kobject = filter(
					lambda x: os.path.exists(x), 
					('/sys/block/sda1', '/sys/block/sda/sda1')
				)
				if not sda1_kobject:
					raise HandlerError('Cannot find sda1 kobject in sysfs')
				root_device_size = int(read_file(sda1_kobject[0] + '/size')) * 512 # Size in bytes
				
				strategy = self._instance_store_strategy_cls(
					role_name, image_name, excludes,
					image_size = root_device_size / 1024 / 1024,
					s3_bucket_name = self._s3_bucket_name
				)

			# Last moment before rebundle
			self._before_rebundle(role_name)			
			bus.fire("before_rebundle", role_name=role_name)
			
			# Run rebundle
			ami_id = strategy.run()
			
			# Software list creation
			software_list = []			
			installed_list = software.all_installed()
			for software_info in installed_list:
				software_list.append(dict(name 	 = software_info.name,
									      version = '.'.join([str(x) for x in software_info.version]),
									      string_version = software_info.string_version
									      ))
			
			os_info = {}
			os_info['version'] = ' '.join(disttool.linux_dist())
			os_info['string_version'] = ' '.join(disttool.uname()).strip()
			
			# Notify Scalr
			self.send_message(Messages.REBUNDLE_RESULT, dict(
				status = "ok",
				snapshot_id = ami_id,
				bundle_task_id = message.bundle_task_id,
				software = software_list,
				os = os_info
			))
			
			# Fire 'rebundle'diss
			bus.fire("rebundle", role_name=role_name, snapshot_id=ami_id)
			self._logger.info('Rebundle complete! If you imported this server to Scalr, you can terminate Scalarizr now.')
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			last_error = hasattr(e, "error_message") and e.error_message or str(e)
			# Send message to Scalr
			self.send_message(Messages.REBUNDLE_RESULT, dict(
				status = "error",
				last_error = last_error,
				bundle_task_id = message.bundle_task_id
			))		
			
			# Fire 'rebundle_error'
			bus.fire("rebundle_error", role_name=role_name, last_error=last_error)
			
		finally:
			self._log_hdlr.bundle_task_id = None
			self._logger.removeHandler(self._log_hdlr)
			if strategy:
				strategy.cleanup()
				
	@property
	def _s3_bucket_name(self):
		pl = bus.platform
		return 'scalr2-images-%s-%s' % (pl.get_region(), pl.get_account_id())
		
	def _before_rebundle(self, role_name):
		# Send wall message before rebundling. So console users can run away
		try:
			Popen(['wall', WALL_MESSAGE]).communicate()
		except OSError, e:
			self._logger.warning(e)


class RebundleStratery:
	_logger = None
		
	_role_name = None
	_image_name = None
	_excludes = None
	_volume = None
	_image = None
	
	def __init__(self, role_name, image_name, excludes, volume='/'):
		self._role_name = role_name
		self._image_name = image_name
		self._excludes = excludes
		self._volume = volume
		self._logger = logging.getLogger(__name__)
	
	def _is_super_user(self):
		return system('id -u')[0].strip() == '0'
	
	def _bundle_vol(self, image):
		try:
			self._logger.info('Bundling volume %s', self._volume)
			
			self._logger.debug("Checking that user is root")
			if not self._is_super_user():
				raise HandlerError("You need to be root to run rebundle")
			self._logger.debug("User check success")
			
			# Create image from volume
			self._logger.debug('Exclude list: %s', image.excludes)			
			image.make()
			self._logger.info("Volume bundle complete!")

		except (Exception, BaseException), e:
			self._logger.error("Cannot bundle volume. %s", e)
			raise
	
	def _create_motd(self, image_mpoint, role_name=None):
		# Create message of the day
		for name in ("etc/motd", "etc/motd.tail"):
			motd_filename = os.path.join(image_mpoint, name)
			if os.path.exists(motd_filename):
				dist = disttool.linux_dist()
				motd = MOTD % dict(
					dist_name = dist[0],
					dist_version = dist[1],
					bits = 64 if disttool.uname()[4] == "x86_64" else 32,
					role_name = role_name,
					bundle_date = datetime.today().strftime("%Y-%m-%d %H:%M")
				)
				filetool.write_file(motd_filename, motd, error_msg="Cannot patch motd file '%s' %s %s")

	def _fix_fstab(self, image_mpoint):
		pl = bus.platform	
		fstab = fstool.Fstab(os.path.join(image_mpoint, 'etc/fstab'), True)		
		
		# Remove EBS volumes from fstab	
		ec2_conn = pl.new_ec2_conn()
		instance = ec2_conn.get_all_instances([pl.get_instance_id()])[0].instances[0]
		
		ebs_devs = list(vol.attach_data.device 
					for vol in ec2_conn.get_all_volumes() 
					if vol.attach_data and vol.attach_data.instance_id == pl.get_instance_id() 
						and instance.root_device_name != vol.attach_data.device)
		
		for devname in ebs_devs:
			fstab.remove(devname, autosave=False)
			
		# Ubuntu 10.04 mountall workaround
		# @see https://bugs.launchpad.net/ubuntu/+source/mountall/+bug/649591
		# @see http://alestic.com/2010/09/ec2-bug-mountall
		if disttool.is_ubuntu():
			try:
				mnt = fstab.find(mpoint='/mnt')[0]
				if mnt.options.find('nobootwait') >= 0:			
					mnt.options = re.sub(r'(nobootwait),(\S+)', r'\2,\1', mnt.options)
				else:
					mnt.options += ',nobootwait'
			except IndexError:
				pass
			
		fstab.save()

	
	def _cleanup_image(self, image_mpoint, role_name=None):
		# Create message of the day
		self._create_motd(image_mpoint, role_name)
		self._fix_fstab(image_mpoint)
		
		# Truncate logs
		logs_path = os.path.join(image_mpoint, "var/log")
		for basename in os.listdir(logs_path):
			filename = os.path.join(logs_path, basename)
			if os.path.isfile(filename):
				try:
					filetool.truncate(filename)
				except OSError, e:
					self._logger.error("Cannot truncate file '%s'. %s", filename, e)
		
		# Cleanup user activity
		for filename in ("root/.bash_history", "root/.lesshst", "root/.viminfo", 
						"root/.mysql_history", "root/.history", "root/.sqlite_history"):
			filename = os.path.join(image_mpoint, filename)
			if os.path.exists(filename):
				os.remove(filename)
		
		# Cleanup scalarizr private data
		etc_path = os.path.join(image_mpoint, bus.etc_path[1:])
		privated = os.path.join(etc_path, "private.d")
		if os.path.exists(privated):
			shutil.rmtree(privated)
			os.mkdir(privated)
		
		bus.fire("rebundle_cleanup_image", image_mpoint=image_mpoint)
		
		# Sync filesystem buffers
		Popen(['sync']).communicate()	
	
	
	def run(self):
		'''
		Run instance bundle 
		'''
		pass
	
	def cleanup(self):
		'''
		Perform cleanup after bundle
		'''
		if self._image:
			try:
				self._image.cleanup()
			except (BaseException, Exception), e:
				self._logger.error('Error during cleanup: %s', e)
	
	
class RebundleInstanceStoreStrategy(RebundleStratery):
	_IMAGE_CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB in bytes.
	_NUM_UPLOAD_THREADS = 4
	_MAX_UPLOAD_ATTEMPTS = 5	

	_destination = None
	_image_name = None
	_image_size = None
	_s3_bucket_name = None
	_platform = None
	
	def __init__(self, role_name, image_name, excludes, volume='/', 
				destination='/mnt', image_size=None, s3_bucket_name=None):
		RebundleStratery.__init__(self, role_name, image_name, excludes, volume)
		self._destination = destination
		self._image_size = image_size
		self._s3_bucket_name = s3_bucket_name
		self._platform = bus.platform

	def _get_arch(self):
		arch = disttool.uname()[4]
		if re.search("^i\d86$", arch):
			arch = "i386"
		return arch		

	def _bundle_image(self, name, image_file, user, destination, user_private_key_string, 
					user_cert_string, ec2_cert_string, key=None, iv=None):
		try:
			self._logger.info("Bundling image...")
			# Create named pipes.
			digest_pipe = os.path.join('/tmp', 'ec2-bundle-image-digest-pipe')
			if os.path.exists(digest_pipe):
				os.remove(digest_pipe)
			try:
				os.mkfifo(digest_pipe)
			except:
				self._logger.error("Cannot create named pipe %s", digest_pipe)
				raise
		
			# Load and generate necessary keys.
			name = os.path.basename(image_file)
			manifest_file = os.path.join(destination, name + '.manifest.xml')
			bundled_file_path = os.path.join(destination, name + '.tar.gz.enc')
			try:
				user_public_key = X509.load_cert_string(user_cert_string).get_pubkey()
			except:
				self._logger.error("Cannot read user EC2 certificate")
				raise
			try:
				user_private_key = RSA.load_key_string(user_private_key_string)
			except:
				self._logger.error("Cannot read user EC2 private key")
				raise
			try:
				ec2_public_key = X509.load_cert_string(ec2_cert_string).get_pubkey()
			except:
				self._logger.error("Cannot read EC2 certificate")
				raise
			key = key or hexlify(Rand.rand_bytes(16))
			iv = iv or hexlify(Rand.rand_bytes(8))
			self._logger.debug('Key: %s', key)
			self._logger.debug('IV: %s', iv)
	
	
			# Bundle the AMI.
			# The image file is tarred - to maintain sparseness, gzipped for
			# compression and then encrypted with AES in CBC mode for
			# confidentiality.
			# To minimize disk I/O the file is read from disk once and
			# piped via several processes. The tee is used to allow a
			# digest of the file to be calculated without having to re-read
			# it from disk.
			openssl = "/usr/sfw/bin/openssl" if disttool.is_sun() else "openssl"
			tar = filetool.Tar()
			tar.create().dereference().sparse()
			tar.add(os.path.basename(image_file), os.path.dirname(image_file))
			digest_file = os.path.join('/tmp', 'ec2-bundle-image-digest.sha1')
			
			self._logger.info("Encrypting image")
			system(" | ".join([
				"%(openssl)s %(digest_algo)s -out %(digest_file)s < %(digest_pipe)s & %(tar)s", 
				"tee %(digest_pipe)s",  
				"gzip", 
				"%(openssl)s enc -e -%(crypto_algo)s -K %(key)s -iv %(iv)s > %(bundled_file_path)s"]) % dict(
					openssl=openssl, digest_algo=DIGEST_ALGO, digest_file=digest_file, digest_pipe=digest_pipe, 
					tar=str(tar), crypto_algo=CRYPTO_ALGO, key=key, iv=iv, bundled_file_path=bundled_file_path
			))
			
			try:
				# openssl produce different outputs:
				# (stdin)= 8ac0626e9a8d54e46e780149a95695ec894449c8
				# 8ac0626e9a8d54e46e780149a95695ec894449c8
				raw_digest = open(digest_file).read()
				digest = raw_digest.split(" ")[-1].strip()
			except IndexError, e:
				self._logger.error("Cannot extract digest from string '%s'", raw_digest)
				raise
			except OSError, e:
				self._logger.error("Cannot read file with image digest '%s'. %s", digest_file, e)
				raise
			finally:
				os.remove(digest_file)
			
			#digest = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
	
			# Split the bundled AMI. 
			# Splitting is not done as part of the compress, encrypt digest
			# stream, so that the filenames of the parts can be easily
			# tracked. The alternative is to create a dedicated output
			# directory, but this leaves the user less choice.
			self._logger.info("Splitting image into chunks")
			part_names = filetool.split(bundled_file_path, name, self._IMAGE_CHUNK_SIZE, destination)
			self._logger.debug("Image splitted into %s chunks", len(part_names))			
			
			# Sum the parts file sizes to get the encrypted file size.
			bundled_size = 0
			for part_name in part_names:
				bundled_size += os.path.getsize(os.path.join(destination, part_name))
			self._logger.debug('Image size: %d bytes', bundled_size)
	
			
			# Encrypt key and iv.
			self._logger.info("Encrypting keys")
			padding = RSA.pkcs1_padding
			user_encrypted_key = hexlify(user_public_key.get_rsa().public_encrypt(key, padding))
			ec2_encrypted_key = hexlify(ec2_public_key.get_rsa().public_encrypt(key, padding))
			user_encrypted_iv = hexlify(user_public_key.get_rsa().public_encrypt(iv, padding))
			ec2_encrypted_iv = hexlify(ec2_public_key.get_rsa().public_encrypt(iv, padding))
			self._logger.debug("Keys encrypted")
			
			# Digest parts.		
			parts = self._digest_parts(part_names, destination)
			
			# Create bundle manifest
			manifest = AmiManifest(
				name=name,
				user=user, 
				arch=self._get_arch(), 
				parts=parts, 
				image_size=os.path.getsize(image_file), 
				bundled_size=bundled_size, 
				user_encrypted_key=user_encrypted_key, 
				ec2_encrypted_key=ec2_encrypted_key, 
				user_encrypted_iv=user_encrypted_iv, 
				ec2_encrypted_iv=ec2_encrypted_iv, 
				image_digest=digest, 
				user_private_key=user_private_key, 
				kernel_id=self._platform.get_kernel_id(), 
				ramdisk_id=self._platform.get_ramdisk_id(), 
				ancestor_ami_ids=self._platform.get_ancestor_ami_ids(), 
				block_device_mapping=self._platform.block_devs_mapping()
			)
			manifest.save(manifest_file)
			
			self._logger.info("Image bundle complete!")
			return manifest_file, manifest
		except (Exception, BaseException), e:
			self._logger.error("Cannot bundle image. %s", e)
			raise
	

	def _digest_parts(self, part_names, destination):
		self._logger.info("Generating digests for each chunk")
		part_digests = []
		for part_name in part_names:
			part_filename = os.path.join(destination, part_name)
			f = None
			try:
				f = open(part_filename)
				digest = EVP.MessageDigest(DIGEST_ALGO)
				part_digests.append((part_name, hexlify(cryptotool.digest_file(digest, f)))) 
			except Exception, BaseException:
				self._logger.error("Cannot generate digest for chunk '%s'", part_name)
				raise
			finally:
				if f is not None:
					f.close()
		return part_digests


	def _upload_image(self, bucket_name, manifest_path, manifest, region=None, acl="aws-exec-read"):
		try:
			self._logger.info("Uploading bundle")
			s3_conn = self._platform.new_s3_conn()
			
			# Create bucket
			bucket = None
			try:
				self._logger.info("Lookup bucket '%s'", bucket_name)
				try:
					# Lockup bucket
					bucket = s3_conn.get_bucket(bucket_name)
					self._logger.debug("Bucket '%s' already exists", bucket_name)
				except:
					# It's important to lockup bucket before creating it because if bucket exists
					# and account has reached buckets limit S3 returns error.
					self._logger.info("Creating bucket '%s'", bucket_name)					
					bucket = s3_conn.create_bucket(bucket_name, location=s3tool.location_from_region(region), policy=acl)
					#bucket = s3_conn.create_bucket(bucket_name, policy=acl)
					
			except BotoServerError, e:
				raise BaseException("Cannot lookup bucket '%s'. %s" % (bucket_name, e.error_message))
			
			# Create files queue
			self._logger.debug("Enqueue files to upload")
			manifest_dir = os.path.dirname(manifest_path)
			upload_files = [manifest_path]
			for part in manifest.parts:
				upload_files.append(os.path.join(manifest_dir, part[0]))
							
			# Start uploaders
			self._logger.info("Uploading files")
			uploader = s3tool.S3Uploader(pool=4, max_attempts=5)
			uploader.upload(upload_files, bucket, s3_conn, acl)
			
			return os.path.join(bucket_name, os.path.basename(manifest_path))
			
		except (Exception, BaseException):
			self._logger.error("Cannot upload image")
			raise

	
	def _register_image(self, s3_manifest_path):
		try:
			self._logger.info("Registering image '%s'", s3_manifest_path)
			ec2_conn = self._platform.new_ec2_conn()

			# @see http://code.google.com/p/boto/issues/detail?id=323			
			#ami_id = ec2_conn.register_image(None, None, image_location=s3_manifest_path)
			rs = ec2_conn.get_object('RegisterImage', {"ImageLocation" : s3_manifest_path}, ResultSet)
			ami_id = getattr(rs, 'imageId', None)
			
			self._logger.info("Registration complete!")
			self._logger.debug('Image %s available', ami_id)
			return ami_id
		except (BaseException, Exception), e:
			self._logger.error("Cannot register image on EC2. %s", e)
			raise


	def run(self):
		image_file = os.path.join(self._destination, self._image_name)
		self._image = LinuxLoopbackImage(self._volume, image_file, self._image_size, self._excludes)
		self._bundle_vol(self._image)
		
		# Clean up 
		self._cleanup_image(self._image.mpoint, self._role_name)
		
		# Bundle image
		cert, pk = self._platform.get_cert_pk()
		manifest_path, manifest = self._bundle_image(
					self._image_name, image_file, self._platform.get_account_id(), 
					self._destination, pk, cert, self._platform.get_ec2_cert())
		
		# Upload image to S3
		s3_manifest_path = self._upload_image(self._s3_bucket_name, manifest_path, 
					manifest, region=self._platform.get_region())
		
		# Register image on EC2
		return self._register_image(s3_manifest_path)
			
	def cleanup(self):
		RebundleStratery.cleanup(self)
		if self._image:
			for path in glob.glob(self._image.path + "*"):
				try:
					if os.path.isdir(path):
						shutil.rmtree(path, ignore_errors=True)
					else:
						os.remove(path)
				except (OSError, IOError), e:
					self._logger.error("Error during cleanup. %s", e)
							

class RebundleEbsStrategy(RebundleStratery):
	_devname = None
	_volsize = None
	_volume_id = None
	_platform = None
	_snap = None
	
	_succeed = None
	
	def __init__(self, role_name, image_name, excludes, volume='/', 
				volume_id=None, volume_size=None, devname='/dev/sdr'):
		RebundleStratery.__init__(self, role_name, image_name, excludes, volume)
		self._devname = devname
		self._volume_id = volume_id
		self._volsize = volume_size
		self._platform = bus.platform

	
	def _create_shapshot(self):
		self._image.umount() 
		vol = self._image.ebs_volume
		self._logger.info('Creating snapshot of root device image %s', vol.id)
		self._snap = vol.create_snapshot("Role %s root device created from %s" 
					% (self._role_name, self._platform.get_instance_id()))

		self._logger.debug('Checking that snapshot %s is completed', self._snap.id)
		wait_until(lambda: self._snap.update() and self._snap.status == 'completed', logger=self._logger)
		self._logger.debug('Snapshot %s completed', self._snap.id)
		
		self._logger.info('Snapshot %s of root device image %s created', self._snap.id, vol.id)
		return self._snap
	
	def _register_image(self):
		root_dev_name = '/dev/sda1'
		root_dev_type = EBSBlockDeviceType()
		root_dev_type.snapshot_id = self._snap.id
		root_dev_type.delete_on_termination = True
		bdmap = BlockDeviceMapping(self._ec2_conn)
		bdmap[root_dev_name] = root_dev_type
		for virtual_name, dev_name in self._platform.get_block_device_mapping().items():
			if virtual_name.startswith('ephemeral'):
				bdmap[dev_name] = virtual_name

		self._logger.info('Registering image')		
		ami_id = self._ec2_conn.register_image(self._image_name, architecture=disttool.arch(), 
				kernel_id=self._platform.get_kernel_id(), ramdisk_id=self._platform.get_ramdisk_id(),
				root_device_name=root_dev_name, block_device_map=bdmap)
			
		self._logger.info('Checking that %s is available', ami_id)
		def check_image():
			try:
				return self._ec2_conn.get_all_images([ami_id])[0].state == 'available'
			except BotoServerError, e:
				if e.error_code == 'InvalidAMIID.NotFound':
					# Sometimes it takes few seconds for EC2 to propagate new AMI
					return False
				raise
			
		wait_until(check_image, logger=self._logger)
		self._logger.debug('Image %s available', ami_id)
		
		self._logger.info('Image registered and available for use!')
		return ami_id

	
	def run(self):
		self._succeed = False
		
		# Bundle image
		self._ec2_conn = self._platform.new_ec2_conn()
		self._image = EbsImage(self._volume, self._devname, self._ec2_conn,
					self._platform.get_avail_zone(), self._platform.get_instance_id(),
					self._volsize, self._volume_id, self._excludes) 
		
		self._bundle_vol(self._image)
		
		# Clean up 
		self._cleanup_image(self._image.mpoint, self._role_name)		
		
		# Create snapshot from root device image
		self._create_shapshot()
		
		# Registering image
		ami_id = self._register_image()
		
		self._succeed = True
		return ami_id
			
	def cleanup(self):
		RebundleStratery.cleanup(self)
		if not self._succeed and self._snap:
			self._logger.debug('Deleting snapshot %s', self._snap.id)
			self._snap.delete()

if disttool.is_linux():
	class LinuxImage:
		SPECIAL_DIRS = ('/dev', '/media', '/mnt', '/proc', '/sys', '/cdrom', '/tmp')
	
		_logger = None
		
		_volume = None
		
		path = None
		'''
		Image file
		'''
		
		devname = None
		'''
		Image device name
		Returned by _create_image def
		'''
		
		mpoint = None
		'''
		Image mount point
		'''
			
		excludes = None
		'''
		Directories excludes list
		'''
		
		_excluded_mpoints = None
		
		_mtab = None
		
		def __init__(self, volume, path, excludes=None):
			self._logger = logging.getLogger(__name__)
			self._mtab = fstool.Mtab()
			self._volume = volume
			self.mpoint = '/mnt/img-mnt'
			self.path = path
			
			# Create rsync excludes list
			self.excludes = set(self.SPECIAL_DIRS) 	# Add special dirs
			self.excludes.update(excludes or ()) 	# Add user input
			self.excludes.add(self.mpoint) 			# Add image mount point
			self.excludes.add(self.path) 			# Add image path
			# Add all mounted filesystems, except bundle volume 
			self._excluded_mpoints = list(entry.mpoint
					for entry in self._mtab.list_entries() 
					if entry.mpoint.startswith(self._volume) and entry.mpoint != self._volume)
			self.excludes.update(self._excluded_mpoints)

		
		def make(self):
			self.devname = self._create_image()
			self._format_image()
			system("sync")  # Flush so newly formatted filesystem is ready to mount.
			self._mount_image()
			self._make_special_dirs()
			self._copy_rec(self._volume, self.mpoint)
			system("sync")  # Flush buffers
			return self.mpoint
		
		
		def cleanup(self):
			self.umount()
			if os.path.exists(self.mpoint):
				os.rmdir(self.mpoint)
		
		def umount(self):
			if self._mtab.contains(mpoint=self.mpoint, reload=True):
				self._logger.debug("Unmounting '%s'", self.mpoint)				
				system("umount -d " + self.mpoint)
		
		def _format_image(self):
			self._logger.info("Formatting image")
			vol_entry = self._mtab.find(mpoint=self._volume)[0]
			system('/sbin/mkfs.%s -F %s 2>&1' % (vol_entry.fstype, self.devname))
			system('/sbin/tune2fs -i 0 %s' % self.devname)
			self._logger.debug('Image %s formatted', self.devname)
			
			# Set volume label
			if vol_entry.fstype in ('ext2', 'ext3', 'ext4'):
				label = system('/sbin/e2label %s' % vol_entry.devname)[0].strip()
				if label:
					self._logger.debug('Set volume label: %s', label)
					system('/sbin/e2label %s %s' % (self.devname, label))


		def _create_image(self):
			pass


		def _mount_image(self, options=None):
			self._logger.info("Mounting image")
			if self._mtab.contains(mpoint=self.mpoint):
				raise HandlerError("Image already mounted")
			fstool.mount(self.devname, self.mpoint, options)
		
		
		def _make_special_dirs(self):
			self._logger.info('Making special directories')
			
			# Create empty special dirs
			for dir in self.SPECIAL_DIRS:
				spec_dir = self.mpoint + dir
				if os.path.exists(dir) and not os.path.exists(spec_dir):
					self._logger.debug("Create spec dir %s", dir)
					os.makedirs(spec_dir)
					if dir == '/tmp':
						os.chmod(spec_dir, 01777)
						
			# Create excluded mpoints dirs (not under special dirs)
			for dir in self._excluded_mpoints:
				if not list(dir for spec_dir in self.SPECIAL_DIRS if dir.startswith(spec_dir)):
					self._logger.debug('Create mpoint dir %s', dir)
					os.makedirs(self.mpoint + dir)
			
			# MAKEDEV is incredibly variable across distros, so use mknod directly.
			dev_dir = self.mpoint + "/dev"
			system("mknod " + dev_dir + "/console c 5 1")
			system("mknod " + dev_dir + "/full c 1 7")									
			system("mknod " + dev_dir + "/null c 1 3")
			#system("ln -s null " + dev_dir +"/X0R")		
			system("mknod " + dev_dir + "/zero c 1 5")
			system("mknod " + dev_dir + "/tty c 5 0")
			system("mknod " + dev_dir + "/tty0 c 4 0")
			system("mknod " + dev_dir + "/tty1 c 4 1")
			system("mknod " + dev_dir + "/tty2 c 4 2")
			system("mknod " + dev_dir + "/tty3 c 4 3")
			system("mknod " + dev_dir + "/tty4 c 4 4")
			system("mknod " + dev_dir + "/tty5 c 4 5")
			system("mknod " + dev_dir + "/xvc0 c 204 191")
			
			self._logger.debug("Special directories maked")			
		
		
		def _copy_rec(self, source, dest, xattr=True):
			self._logger.info("Copying %s into the image %s", source, dest)
			rsync = filetool.Rsync()
			#rsync.archive().times().sparse().links().quietly()
			rsync.archive().sparse()
			if xattr:
				rsync.xattributes()
			rsync.exclude(self.excludes)
			rsync.source(source).dest(dest)
			out, err, exitcode = rsync.execute()
			self._logger.debug('rsync stdout: %s', out)
			self._logger.debug('rsync stderr: %s', err)
			
			if exitcode == 23 and filetool.Rsync.usable():
				self._logger.warning(
					"rsync seemed successful but exited with error code 23. This probably means " +
	           		"that your version of rsync was built against a kernel with HAVE_LUTIMES defined, " +
	             	"although the current kernel was not built with this option enabled. The bundling " +
				 	"process will thus ignore the error and continue bundling.  If bundling completes " +
	           		"successfully, your image should be perfectly usable. We, however, recommend that " +
			   		"you install a version of rsync that handles this situation more elegantly.")
			elif exitcode == 1 and xattr:
				self._logger.warning(
					"rsync with preservation of extended file attributes failed. Retrying rsync " +
	           		"without attempting to preserve extended file attributes...")
				self._copy_rec(source, dest, xattr=False)
			elif exitcode > 0:
				raise HandlerError('rsync failed with exit code %s' % (exitcode,))
		
		
	
	class LinuxEbsImage(LinuxImage):
		'''
		This class encapsulate functionality to create a EBS from a root volume 
		'''
		_ec2_conn = None
		_avail_zone = None
		_instance_id = None
		_volume_size = None
		ebs_volume = None
				
		def __init__(self, volume, devname, ec2_conn, avail_zone, instance_id, 
					volume_size=None, volume_id=None, excludes=None):
			LinuxImage.__init__(self, volume, devname, excludes)
			self.devname = devname
			self._ec2_conn = ec2_conn
			self._avail_zone = avail_zone
			self._instance_id = instance_id
			if volume_id:
				self.ebs_volume = Volume(self._ec2_conn)
				self.ebs_volume.id = volume_id
			else:		
				self._volume_size = volume_size

		def _create_image(self):
			if not self.ebs_volume:
				self.ebs_volume = ebstool.create_volume(self._ec2_conn, self._volume_size, 
						self._avail_zone, logger=self._logger)
			ebstool.attach_volume(self._ec2_conn, self.ebs_volume, 
					self._instance_id, self.devname, to_me=True, logger=self._logger)
			return self.devname
			
		def make(self):
			self._logger.info("Make EBS volume %s from volume %s (excludes: %s)", 
					self.devname, self._volume, ":".join(self.excludes))
			LinuxImage.make(self)
			
		def cleanup(self):
			LinuxImage.cleanup(self)
			if self.ebs_volume:
				ebstool.detach_volume(self._ec2_conn, self.ebs_volume, logger=self._logger)
				ebstool.delete_volume(self._ec2_conn, self.ebs_volume)
				self.ebs_volume = None

	
	class LinuxLoopbackImage(LinuxImage):
		"""
		This class encapsulate functionality to create an file loopback image
		from a volume. The image is created using dd. Sub-directories of the 
		volume, including mounts of local filesystems, are copied to the image. 
		Symbolic links are preserved. 	
		"""
		
		MAX_IMAGE_SIZE = 10*1024
		_size = None
		
		def __init__(self, volume, image_file, image_size, excludes=None):
			'''
			@param volume: Path to mounted volume to create the bundle from. Ex: '/'
			@param image_file:  Destination file to store the bundled image. Ex: /mnt/img
			@param image_size: Image file size in Mb. Ex: 1408 (1Gb)
			@param excludes: list of directories and files to exclude. Ex: /mnt, /root/.*
			'''
			LinuxImage.__init__(self, volume, image_file, excludes)
			self._size = image_size or self.MAX_IMAGE_SIZE
		
		def make(self):
			self._logger.info("Make image %s from volume %s (excludes: %s)", 
					self.path, self._volume, ":".join(self.excludes))
			LinuxImage.make(self)
			
		def _create_image(self):
			self._logger.debug('Creating image file %s', self.path)
			system("dd if=/dev/zero of='%s' bs=1M count=1 seek=%s" % (self.path, self._size - 1))
			self._logger.debug('Image file %s created', self.path)			
			
			self._logger.debug('Associate loop device with a %s', self.path)
			devname = system('/sbin/losetup -f')[0].strip()
			out, err, retcode = system('/sbin/losetup %s "%s"' % (devname, self.path))
			if retcode > 0:
				raise HandlerError('Cannot setup loop device. Code: %d %s' % (retcode, err))
			self._logger.debug('Associated %s with a file %s', devname, self.path)
			
			return devname
			
		def cleanup(self):
			LinuxImage.cleanup(self)
			if self.devname:
				system('/sbin/losetup -d %s' % self.devname)
				
	LoopbackImage = LinuxLoopbackImage
	EbsImage = LinuxEbsImage
	
elif disttool.is_sun():
	# @todo: Solaris support
	class SolarisLoopbakImage: pass
	class SolarisEbsImage: pass
	
	LoopbackImage = SolarisLoopbakImage
	EbsImage = SolarisEbsImage



class AmiManifest:
	
	VERSION = "2007-10-10"
	
	name = None
	user = None
	arch = None
	parts = None
	image_size = None
	bundled_size=None
	bundler_name=None,
	bundler_version=None,
	bundler_release=None,
	user_encrypted_key=None 
	ec2_encrypted_key=None
	user_encrypted_iv=None
	ec2_encrypted_iv=None
	image_digest=None
	digest_algo=None
	crypto_algo=None
	user_private_key=None 
	kernel_id=None
	ramdisk_id=None
	product_codes=None
	ancestor_ami_ids=None 
	block_device_mapping=None	
	
	_logger = None
	
	def __init__(self, name=None, user=None, arch=None, 
				parts=None, image_size=None, bundled_size=None, user_encrypted_key=None, 
				ec2_encrypted_key=None,	user_encrypted_iv=None,	ec2_encrypted_iv=None, 
				image_digest=None, digest_algo=DIGEST_ALGO, crypto_algo=CRYPTO_ALGO, 
				bundler_name=BUNDLER_NAME, bundler_version=BUNDLER_VERSION, bundler_release=BUNDLER_RELEASE,
				user_private_key=None, kernel_id=None, ramdisk_id=None, product_codes=None, 
				ancestor_ami_ids=None, block_device_mapping=None):
		for key, value in locals().items():
			if key != "self" and hasattr(self, key):
				setattr(self, key, value)
		self._logger = logging.getLogger(__name__)
	
	def save(self, filename):
		self._logger.info("Generating manifest file '%s'", filename)

		out_file = open(filename, "wb")
		doc = Document()

		def el(name):
			return doc.createElement(name)
		def txt(text):
			return doc.createTextNode('%s' % (text))
		def ap(parent, child):
			parent.appendChild(child)

		manifest_elem = el("manifest")
		ap(doc, manifest_elem)

		#version
		# /manifest/version
		version_elem = el("version")
		version_value = txt(self.VERSION)
		ap(version_elem, version_value)
		ap(manifest_elem, version_elem)

		#bundler info
		# /manifest/bundler
		bundler_elem = el("bundler")
		
		bundler_name_elem = el("name")
		bundler_name_value = txt(self.bundler_name)
		ap(bundler_name_elem, bundler_name_value)
		ap(bundler_elem, bundler_name_elem)		
		
		bundler_version_elem = el("version")
		bundler_version_value = txt(self.bundler_version)
		ap(bundler_version_elem, bundler_version_value)
		ap(bundler_elem, bundler_version_elem)
		
		release_elem = el("release")
		release_value = txt(self.bundler_release)
		ap(release_elem, release_value)
		ap(bundler_elem, release_elem)
		
		ap(manifest_elem, bundler_elem)


		#machine config
		# /manifest/machine_configuration
		machine_config_elem = el("machine_configuration")
		ap(manifest_elem, machine_config_elem)
		
		arch_elem = el("architecture")
		arch_value = txt(self.arch)
		ap(arch_elem, arch_value)
		ap(machine_config_elem, arch_elem)


		#block device mapping
		# /manifest/machine_configuration/block_device_mapping
		if self.block_device_mapping:
			block_dev_mapping_elem = el("block_device_mapping")
			for virtual, device in self.block_device_mapping:
				mapping_elem = el("mapping")
				
				virtual_elem = el("virtual")
				virtual_value = txt(virtual)
				ap(virtual_elem, virtual_value)
				ap(mapping_elem, virtual_elem)
				
				device_elem = el("device")
				device_value = txt(device)
				ap(device_elem, device_value)
				ap(mapping_elem, device_elem)
				
				ap(block_dev_mapping_elem, mapping_elem)
				
			ap(machine_config_elem, block_dev_mapping_elem)

		# /manifest/machine_configuration/product_codes
		if self.product_codes:
			product_codes_elem = el("product_codes")
			for product_code in self.product_codes:
				product_code_elem = el("product_code");
				product_code_value = txt(product_code)
				ap(product_code_elem, product_code_value)
				ap(product_codes_elem, product_code_elem)
			ap(machine_config_elem, product_codes_elem)


		#kernel and ramdisk
		# /manifest/machine_configuration/kernel_id
		if self.kernel_id:
			kernel_id_elem = el("kernel_id")
			kernel_id_value = txt(self.kernel_id)
			ap(kernel_id_elem, kernel_id_value)
			ap(machine_config_elem, kernel_id_elem)
			
		# /manifest/machine_configuration/ramdisk_id
		if self.ramdisk_id:
			ramdisk_id_elem = el("ramdisk_id")
			ramdisk_id_value = txt(self.ramdisk_id)
			ap(ramdisk_id_elem, ramdisk_id_value)
			ap(machine_config_elem, ramdisk_id_elem)


		# /manifest/image
		image_elem = el("image")
		ap(manifest_elem, image_elem)

		#name
		# /manifest/image/name
		image_name_elem = el("name") 
		image_name_value = txt(self.name)
		ap(image_name_elem, image_name_value)
		ap(image_elem, image_name_elem)

		#user
		# /manifest/image/user
		user_elem = el("user")
		user_value = txt(self.user)
		ap(user_elem, user_value)
		ap(image_elem, user_elem)

		#type
		# /manifest/image/type
		image_type_elem = el("type")
		image_type_value = txt("machine")
		ap(image_type_elem, image_type_value)
		ap(image_elem, image_type_elem)


		#ancestor ami ids 
		# /manifest/image/ancestry
		if self.ancestor_ami_ids:
			ancestry_elem = el("ancestry")
			for ancestor_ami_id in self.ancestor_ami_ids:
				ancestor_id_elem = el("ancestor_ami_id");
				ancestor_id_value = txt(ancestor_ami_id)
				ap(ancestor_id_elem, ancestor_id_value)
				ap(ancestry_elem, ancestor_id_elem)
			ap(image_elem, ancestry_elem)

		#digest
		# /manifest/image/digest
		image_digest_elem = el("digest")
		image_digest_elem.setAttribute('algorithm', self.digest_algo.upper())
		image_digest_value = txt(self.image_digest)
		ap(image_digest_elem, image_digest_value)
		ap(image_elem, image_digest_elem)

		#size
		# /manifest/image/size
		image_size_elem = el("size")
		image_size_value = txt(self.image_size)
		ap(image_size_elem, image_size_value)
		ap(image_elem, image_size_elem)

		#bundled size
		# /manifest/image/bundled_size
		bundled_size_elem = el("bundled_size")
		bundled_size_value = txt(self.bundled_size)
		ap(bundled_size_elem, bundled_size_value)
		ap(image_elem, bundled_size_elem)

		#key, iv
		# /manifest/image/ec2_encrypted_key
		ec2_encrypted_key_elem = el("ec2_encrypted_key")
		ec2_encrypted_key_value = txt(self.ec2_encrypted_key)
		ec2_encrypted_key_elem.setAttribute("algorithm", self.crypto_algo.upper())		
		ap(ec2_encrypted_key_elem, ec2_encrypted_key_value)
		ap(image_elem, ec2_encrypted_key_elem)
		
		# /manifest/image/user_encrypted_key
		user_encrypted_key_elem = el("user_encrypted_key")
		user_encrypted_key_value = txt(self.user_encrypted_key)
		user_encrypted_key_elem.setAttribute("algorithm", self.crypto_algo.upper())		
		ap(user_encrypted_key_elem, user_encrypted_key_value)
		ap(image_elem, user_encrypted_key_elem)

		# /manifest/image/ec2_encrypted_iv
		ec2_encrypted_iv_elem = el("ec2_encrypted_iv")
		ec2_encrypted_iv_value = txt(self.ec2_encrypted_iv)
		ap(ec2_encrypted_iv_elem, ec2_encrypted_iv_value)
		ap(image_elem, ec2_encrypted_iv_elem)

		# /manifest/image/user_encrypted_iv
		user_encrypted_iv_elem = el("user_encrypted_iv")
		user_encrypted_iv_value = txt(self.user_encrypted_iv)
		ap(user_encrypted_iv_elem, user_encrypted_iv_value)
		ap(image_elem, user_encrypted_iv_elem)


		#parts
		# /manifest/image/parts
		parts_elem = el("parts")
		parts_elem.setAttribute("count", str(len(self.parts)))
		part_number = 0
		for part in self.parts:
			part_elem = el("part")
			filename_elem = el("filename")
			filename_value = txt(part[0])
			ap(filename_elem, filename_value)
			ap(part_elem, filename_elem)
			
			#digest
			part_digest_elem = el("digest")
			part_digest_elem.setAttribute('algorithm', self.digest_algo.upper())
			part_digest_value = txt(part[1])
			ap(part_digest_elem, part_digest_value)
			ap(part_elem, part_digest_elem)
			part_elem.setAttribute("index", str(part_number))
			
			ap(parts_elem, part_elem)
			part_number += 1
		ap(image_elem, parts_elem)

		
		# Get the XML for <machine_configuration> and <image> elements and sign them.
		string_to_sign = machine_config_elem.toxml() + image_elem.toxml()
		
		digest = EVP.MessageDigest(self.digest_algo.lower())
		digest.update(string_to_sign)
		sig = hexlify(self.user_private_key.sign(digest.final()))
		del digest
		
		# /manifest/signature
		signature_elem = el("signature")
		signature_value = txt(sig)
		ap(signature_elem, signature_value)
		ap(manifest_elem, signature_elem)

		out_file.write(doc.toxml())
		out_file.close()

	
	def load(self, filename):
		# TODO: implement
		pass
	
	def startElement(self, name, attrs):
		pass
	
	def characters(self, value):
		pass
	
	def endElement(self, name):
		pass


class RebundleLogHandler(logging.Handler):
	def __init__(self, bundle_task_id=None):
		logging.Handler.__init__(self)
		self.bundle_task_id = bundle_task_id
		self._msg_service = bus.messaging_service
		
	def emit(self, record):
		msg = self._msg_service.new_message(Messages.REBUNDLE_LOG, body=dict(
			bundle_task_id = self.bundle_task_id,
			message = str(record.msg) % record.args if record.args else str(record.msg)
		))
		self._msg_service.get_producer().send(Queues.LOG, msg)
