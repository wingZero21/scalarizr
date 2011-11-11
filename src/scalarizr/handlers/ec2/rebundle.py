'''
Created on Mar 11, 2010
@author: marat
'''

import scalarizr
from scalarizr.bus import bus
from scalarizr.handlers import HandlerError
from scalarizr.util import system2, disttool, cryptotool, fstool, filetool,\
	wait_until, get_free_devname, firstmatched
from scalarizr.platform.ec2 import ebstool
from scalarizr.storage.transfer import Transfer
from scalarizr.handlers import rebundle as rebundle_hdlr


from M2Crypto import X509, EVP, Rand, RSA
from binascii import hexlify
from xml.dom.minidom import Document
from datetime import datetime
import time, os, re, shutil, glob

from boto.exception import BotoServerError
from boto.ec2.volume import Volume
from boto.ec2.blockdevicemapping import EBSBlockDeviceType, BlockDeviceMapping


# Workaround for python bug #5853
# @see http://bugs.python.org/issue5853
# @see http://groups.google.com/group/smug-dev/browse_thread/thread/47e7833edb9efbf9?pli=1
import mimetypes
mimetypes.init()


def get_handlers ():
	return [Ec2RebundleHandler()]

LOG = rebundle_hdlr.LOG

BUNDLER_NAME = "scalarizr"
BUNDLER_VERSION = scalarizr.__version__
BUNDLER_RELEASE = "672"

DIGEST_ALGO = "sha1"
CRYPTO_ALGO = "aes-128-cbc"

EPH_STORAGE_MAPPING = {
	'i386': {
		'ephemeral0': '/dev/sda2',
	},
	'x86_64': {
		'ephemeral0': '/dev/sdb',
		'ephemeral1': '/dev/sdc',
		'ephemeral2': '/dev/sdd',
		'ephemeral3': '/dev/sde',
	}
} 


class Ec2RebundleHandler(rebundle_hdlr.RebundleHandler):
	_ebs_strategy_cls = None
	_instance_store_strategy_cls = None
	
	def __init__(self, ebs_strategy_cls=None, instance_store_strategy_cls=None):
		rebundle_hdlr.RebundleHandler.__init__(self)
		
		self._ebs_strategy_cls = ebs_strategy_cls or RebundleEbsStrategy
		self._instance_store_strategy_cls = instance_store_strategy_cls or RebundleInstanceStoreStrategy
		
		
	def before_rebundle(self):
		'''
		@param message.volume_size:  
			New size for EBS-root device. 
			By default current EBS-root size will be used (15G in most popular AMIs)	
		@param message.volume_id  
			EBS volume for root device copy.
		'''
	
		image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")

		# Take rebundle strategy
		pl = bus.platform 
		ec2_conn = pl.new_ec2_conn()
		instance = ec2_conn.get_all_instances([pl.get_instance_id()])[0].instances[0]
		
		# Find root device
		root_disk = firstmatched(lambda x: x.mpoint == '/', filetool.df())
		if not root_disk:
			raise HandlerError("Can't find root device")
		
		if instance.root_device_name:
			# EBS-root device instance
			volume_size = self._rebundle_message.body.get('volume_size')			
			if not volume_size:
				volume_size = int(root_disk.size / 1000 / 1000)
			
			self._strategy = self._ebs_strategy_cls(
				self, self._role_name, image_name, self._excludes,
				devname=get_free_devname(), 
				volume_size=volume_size,  # in Gb
				volume_id=self._rebundle_message.body.get('volume_id')
			)
		else:
			# Instance store
			self._strategy = self._instance_store_strategy_cls(
				self, self._role_name, image_name, self._excludes,
				image_size = root_disk.size / 1000,  # in Mb
				s3_bucket_name = self._s3_bucket_name
			)
		
		
	def rebundle(self):
		return self._strategy.run()
		
			
	def after_rebundle(self):
		if self._strategy:
			self._strategy.cleanup()

	@property
	def _s3_bucket_name(self):
		pl = bus.platform
		return 'scalr2-images-%s-%s' % (pl.get_region(), pl.get_account_id())


class RebundleStratery:
		
	_role_name = None
	_image_name = None
	_excludes = None
	_volume = None
	_image = None
	
	def __init__(self, handler, role_name, image_name, excludes, volume='/'):
		self._hdlr = handler
		self._role_name = role_name
		self._image_name = image_name
		self._excludes = excludes
		self._volume = volume
	
	def _is_super_user(self):
		return system2('id -u', shell=True)[0].strip() == '0'
	
	def _bundle_vol(self, image):
		try:
			LOG.info('Bundling volume %s', self._volume)
			
			LOG.debug("Checking that user is root")
			if not self._is_super_user():
				raise HandlerError("You need to be root to run rebundle")
			LOG.debug("User check success")
			
			# Create image from volume
			LOG.debug('Exclude list: %s', image.excludes)			
			image.make()
			LOG.info("Volume bundle complete!")

		except (Exception, BaseException), e:
			LOG.error("Cannot bundle volume. %s", e)
			raise
	
	def _create_motd(self, image_mpoint, role_name=None):
		LOG.debug('Creating motd file')
		# Create message of the day
		for name in ("etc/motd", "etc/motd.tail"):
			motd_filename = os.path.join(image_mpoint, name)
			if os.path.exists(motd_filename):
				dist = disttool.linux_dist()
				motd = rebundle_hdlr.MOTD % dict(
					dist_name = dist[0],
					dist_version = dist[1],
					bits = 64 if disttool.uname()[4] == "x86_64" else 32,
					role_name = role_name,
					bundle_date = datetime.today().strftime("%Y-%m-%d %H:%M")
				)
				filetool.write_file(motd_filename, motd, error_msg="Cannot patch motd file '%s' %s %s")

	def _fix_fstab(self, image_mpoint):
		LOG.debug('Fixing fstab')
		pl = bus.platform	
		fstab = fstool.Fstab(os.path.join(image_mpoint, 'etc/fstab'), True)		
		
		# Remove EBS volumes from fstab	
		ec2_conn = pl.new_ec2_conn()
		instance = ec2_conn.get_all_instances([pl.get_instance_id()])[0].instances[0]
		
		ebs_devs = list(vol.attach_data.device 
					for vol in ec2_conn.get_all_volumes(filters={'attachment.instance-id': pl.get_instance_id()}) 
					if vol.attach_data and vol.attach_data.instance_id == pl.get_instance_id() 
						and instance.root_device_name != vol.attach_data.device)
		
		for devname in ebs_devs:
			fstab.remove(devname, autosave=False)
			
		# Ubuntu 10.04 mountall workaround
		# @see https://bugs.launchpad.net/ubuntu/+source/mountall/+bug/649591
		# @see http://alestic.com/2010/09/ec2-bug-mountall
		if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			for entry in fstab.list_entries():
				if entry.devname in pl.instance_store_devices:
					if entry.options.find('nobootwait') >= 0:			
						entry.options = re.sub(r'(nobootwait),(\S+)', r'\2,\1', entry.options)
					else:
						entry.options += ',nobootwait'
						
		fstab.save()

	
	def _cleanup_image(self, image_mpoint, role_name=None):
		# Create message of the day
		self._create_motd(image_mpoint, role_name)
		self._fix_fstab(image_mpoint)
		self._hdlr.cleanup_image(image_mpoint)
	
	
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
				LOG.error('Error during cleanup: %s', e)
	
	
class RebundleInstanceStoreStrategy(RebundleStratery):
	_IMAGE_CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB in bytes.
	_NUM_UPLOAD_THREADS = 4
	_MAX_UPLOAD_ATTEMPTS = 5	

	_destination = None
	_image_name = None
	_image_size = None
	_s3_bucket_name = None
	_platform = None
	
	def __init__(self, handler, role_name, image_name, excludes, volume='/', 
				destination='/mnt', image_size=None, s3_bucket_name=None):
		RebundleStratery.__init__(self, handler, role_name, image_name, excludes, volume)
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
			LOG.info("Bundling image...")
			# Create named pipes.
			digest_pipe = os.path.join('/tmp', 'ec2-bundle-image-digest-pipe')
			if os.path.exists(digest_pipe):
				os.remove(digest_pipe)
			try:
				os.mkfifo(digest_pipe)
			except:
				LOG.error("Cannot create named pipe %s", digest_pipe)
				raise
		
			# Load and generate necessary keys.
			name = os.path.basename(image_file)
			manifest_file = os.path.join(destination, name + '.manifest.xml')
			bundled_file_path = os.path.join(destination, name + '.tar.gz.enc')
			try:
				user_public_key = X509.load_cert_string(user_cert_string).get_pubkey()
			except:
				LOG.error("Cannot read user EC2 certificate")
				raise
			try:
				user_private_key = RSA.load_key_string(user_private_key_string)
			except:
				LOG.error("Cannot read user EC2 private key")
				raise
			try:
				ec2_public_key = X509.load_cert_string(ec2_cert_string).get_pubkey()
			except:
				LOG.error("Cannot read EC2 certificate")
				raise
			key = key or hexlify(Rand.rand_bytes(16))
			iv = iv or hexlify(Rand.rand_bytes(8))
			LOG.debug('Key: %s', key)
			LOG.debug('IV: %s', iv)
	
	
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
			
			LOG.info("Encrypting image")
			system2(" | ".join([
				"%(openssl)s %(digest_algo)s -out %(digest_file)s < %(digest_pipe)s & %(tar)s", 
				"tee %(digest_pipe)s",  
				"gzip", 
				"%(openssl)s enc -e -%(crypto_algo)s -K %(key)s -iv %(iv)s > %(bundled_file_path)s"]) % dict(
					openssl=openssl, digest_algo=DIGEST_ALGO, digest_file=digest_file, digest_pipe=digest_pipe, 
					tar=str(tar), crypto_algo=CRYPTO_ALGO, key=key, iv=iv, bundled_file_path=bundled_file_path
			), shell=True)
			
			try:
				# openssl produce different outputs:
				# (stdin)= 8ac0626e9a8d54e46e780149a95695ec894449c8
				# 8ac0626e9a8d54e46e780149a95695ec894449c8
				raw_digest = open(digest_file).read()
				digest = raw_digest.split(" ")[-1].strip()
			except IndexError, e:
				LOG.error("Cannot extract digest from string '%s'", raw_digest)
				raise
			except OSError, e:
				LOG.error("Cannot read file with image digest '%s'. %s", digest_file, e)
				raise
			finally:
				os.remove(digest_file)
			
			#digest = "da39a3ee5e6b4b0d3255bfef95601890afd80709"
	
			# Split the bundled AMI. 
			# Splitting is not done as part of the compress, encrypt digest
			# stream, so that the filenames of the parts can be easily
			# tracked. The alternative is to create a dedicated output
			# directory, but this leaves the user less choice.
			LOG.info("Splitting image into chunks")
			part_names = filetool.split(bundled_file_path, name, self._IMAGE_CHUNK_SIZE, destination)
			LOG.debug("Image splitted into %s chunks", len(part_names))			
			
			# Sum the parts file sizes to get the encrypted file size.
			bundled_size = 0
			for part_name in part_names:
				bundled_size += os.path.getsize(os.path.join(destination, part_name))
			LOG.debug('Image size: %d bytes', bundled_size)
	
			
			# Encrypt key and iv.
			LOG.info("Encrypting keys")
			padding = RSA.pkcs1_padding
			user_encrypted_key = hexlify(user_public_key.get_rsa().public_encrypt(key, padding))
			ec2_encrypted_key = hexlify(ec2_public_key.get_rsa().public_encrypt(key, padding))
			user_encrypted_iv = hexlify(user_public_key.get_rsa().public_encrypt(iv, padding))
			ec2_encrypted_iv = hexlify(ec2_public_key.get_rsa().public_encrypt(iv, padding))
			LOG.debug("Keys encrypted")
			
			# Digest parts.		
			parts = self._digest_parts(part_names, destination)
			
			# Create bundle manifest
			bdm = list((name, device) for name, device in self._platform.block_devs_mapping() 
					if not name.startswith('ephemeral'))
			bdm += EPH_STORAGE_MAPPING[disttool.arch()].items()
			
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
				block_device_mapping=bdm
			)
			manifest.save(manifest_file)
			
			LOG.info("Image bundle complete!")
			return manifest_file, manifest
		except (Exception, BaseException), e:
			LOG.error("Cannot bundle image. %s", e)
			raise
	

	def _digest_parts(self, part_names, destination):
		LOG.info("Generating digests for each chunk")
		part_digests = []
		for part_name in part_names:
			part_filename = os.path.join(destination, part_name)
			f = None
			try:
				f = open(part_filename)
				digest = EVP.MessageDigest(DIGEST_ALGO)
				part_digests.append((part_name, hexlify(cryptotool.digest_file(digest, f)))) 
			except Exception, BaseException:
				LOG.error("Cannot generate digest for chunk '%s'", part_name)
				raise
			finally:
				if f is not None:
					f.close()
		return part_digests


	def _upload_image(self, bucket_name, manifest_path, manifest, region=None, acl="aws-exec-read"):
		try:
			LOG.info("Uploading bundle")
			
			# Files to upload
			LOG.debug("Enqueue files to upload")
			manifest_dir = os.path.dirname(manifest_path)			
			upload_files = [manifest_path]
			for part in manifest.parts:
				upload_files.append(os.path.join(manifest_dir, part[0]))
			
			trn = Transfer(pool=4, max_attempts=5, logger=LOG)
			trn.upload(upload_files, 's3://%s/' % bucket_name)
			
			return os.path.join(bucket_name, os.path.basename(manifest_path))
		
			
		except (Exception, BaseException):
			LOG.error("Cannot upload image")
			raise

	
	def _register_image(self, s3_manifest_path):
		try:
			LOG.info("Registering image '%s'", s3_manifest_path)
			ec2_conn = self._platform.new_ec2_conn()

			ami_id = ec2_conn.register_image(image_location=s3_manifest_path)
			# @see http://code.google.com/p/boto/issues/detail?id=323			
			#rs = ec2_conn.get_object('RegisterImage', {"ImageLocation" : s3_manifest_path}, ResultSet)
			#ami_id = getattr(rs, 'imageId', None)
			
			LOG.info("Registration complete!")
			LOG.debug('Image %s available', ami_id)
			return ami_id
		except (BaseException, Exception), e:
			LOG.error("Cannot register image on EC2. %s", e)
			raise


	def run(self):
		image_file = os.path.join(self._destination, self._image_name)
		self._image = rebundle_hdlr.LinuxLoopbackImage(self._volume, image_file, self._image_size, self._excludes)
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
					LOG.error("Error during cleanup. %s", e)
							

class RebundleEbsStrategy(RebundleStratery):
	_devname = None
	_volsize = None
	_volume_id = None
	_platform = None
	_snap = None
	
	_succeed = None
	
	def __init__(self, handler, role_name, image_name, excludes, volume='/', 
				volume_id=None, volume_size=None, devname='/dev/sdr'):
		RebundleStratery.__init__(self, handler, role_name, image_name, excludes, volume)
		self._devname = devname
		self._volume_id = volume_id
		self._volsize = volume_size
		self._platform = bus.platform

	
	def _create_shapshot(self):
		self._image.umount() 
		vol = self._image.ebs_volume
		LOG.info('Creating snapshot of root device image %s', vol.id)
		self._snap = vol.create_snapshot("Root device snapshot created from role: %s instance: %s" 
					% (self._role_name, self._platform.get_instance_id()))

		LOG.debug('Checking that snapshot %s is completed', self._snap.id)
		while True:
			self._snap.update()
			if self._snap.status == 'completed':
				break
			elif self._snap.status == 'failed':
				raise Exception('Snapshot %s status changed to failed on EC2' % (self._snap.id, ))
			LOG.debug('Progress: %s', self._snap.progress)
			time.sleep(5)
		LOG.debug('Snapshot %s completed', self._snap.id)
		
		LOG.info('Snapshot %s of root device image %s created', self._snap.id, vol.id)
		return self._snap
	
	def _register_image(self):
		instance = self._ec2_conn.get_all_instances((self._platform.get_instance_id(), ))[0].instances[0]

		root_device_type = EBSBlockDeviceType()
		root_device_type.snapshot_id = self._snap.id
		root_device_type.delete_on_termination = True

		bdmap = BlockDeviceMapping(self._ec2_conn)

		# Add ephemeral devices
		for eph, device in EPH_STORAGE_MAPPING[disttool.arch()].items():
			bdt = EBSBlockDeviceType(self._ec2_conn)
			bdt.ephemeral_name = eph
			bdmap[device] = bdt
			
		# Add root device snapshot
		root_partition = instance.root_device_name[:-1]
		if root_partition in self._platform.get_block_device_mapping().values():
			bdmap[root_partition] = root_device_type
		else:
			bdmap[instance.root_device_name] = root_device_type
		
		LOG.info('Registering image')		
		ami_id = self._ec2_conn.register_image(self._image_name, architecture=disttool.arch(), 
				kernel_id=self._platform.get_kernel_id(), ramdisk_id=self._platform.get_ramdisk_id(),
				root_device_name=instance.root_device_name, block_device_map=bdmap)
			
		LOG.info('Checking that %s is available', ami_id)
		def check_image():
			try:
				return self._ec2_conn.get_all_images([ami_id])[0].state == 'available'
			except BotoServerError, e:
				if e.error_code == 'InvalidAMIID.NotFound':
					# Sometimes it takes few seconds for EC2 to propagate new AMI
					return False
				raise
		wait_until(check_image, logger=LOG, timeout=3600,
				error_text="Image %s wasn't completed in a reasonable time" % ami_id)
		LOG.debug('Image %s available', ami_id)
		
		LOG.info('Image registered and available for use!')
		return ami_id

	
	def run(self):
		self._succeed = False
		
		# Bundle image
		self._ec2_conn = self._platform.new_ec2_conn()
		self._image = LinuxEbsImage(self._volume, self._devname, self._ec2_conn,
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
			LOG.debug('Deleting snapshot %s', self._snap.id)
			self._snap.delete()

		
		
	
class LinuxEbsImage(rebundle_hdlr.LinuxImage):
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
		rebundle_hdlr.LinuxImage.__init__(self, volume, devname, excludes)
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
					self._avail_zone, logger=LOG, tags={'tmp' : '1'})
		return ebstool.attach_volume(self._ec2_conn, self.ebs_volume, 
				self._instance_id, self.devname, to_me=True, logger=LOG)[1]
		
	def make(self):
		LOG.info("Make EBS volume %s (size: %sGb) from volume %s (excludes: %s)", 
				self.devname, self._volume_size, self._volume, ":".join(self.excludes))
		rebundle_hdlr.LinuxImage.make(self)
		
	def cleanup(self):
		rebundle_hdlr.LinuxImage.cleanup(self)
		if self.ebs_volume:
			ebstool.detach_volume(self._ec2_conn, self.ebs_volume, logger=LOG)
			ebstool.delete_volume(self._ec2_conn, self.ebs_volume)
			self.ebs_volume = None


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

	
	def save(self, filename):
		LOG.info("Generating manifest file '%s'", filename)

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
