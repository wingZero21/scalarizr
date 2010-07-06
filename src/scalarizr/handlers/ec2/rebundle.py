'''
Created on Mar 11, 2010

@author: marat
'''

import scalarizr
from scalarizr.bus import bus
from scalarizr.handlers import Handler, async, HandlerError
from scalarizr.messaging import Messages
from scalarizr.util import system, disttool, cryptotool, fstool, filetool

import logging
import time
import os
import re
from M2Crypto import X509, EVP, Rand, RSA
from binascii import hexlify
from xml.dom.minidom import Document
from datetime import datetime
from threading import Thread, Lock
from Queue import Queue, Empty
import shutil
import glob

from boto.s3 import Key
from boto.s3.connection import Location
from boto.resultset import ResultSet
from boto.exception import BotoServerError

# Workaround for python bug #5853
# @see http://bugs.python.org/issue5853
# @see http://groups.google.com/group/smug-dev/browse_thread/thread/47e7833edb9efbf9?pli=1
import mimetypes
mimetypes.init()

def get_handlers ():
	return [Ec2RebundleHandler()]


BUNDLER_NAME = "scalarizr"
BUNDLER_VERSION = scalarizr.__version__
BUNDLER_RELEASE = "76"
DIGEST_ALGO = "sha1"
CRYPTO_ALGO = "aes-128-cbc"


class Ec2RebundleHandler(Handler):
	_logger = None
	
	_platform = None
	"""
	@ivar scalarizr.platform.ec2.AwsPlatform: 
	"""
	
	_msg_service = None
	
	_IMAGE_CHUNK_SIZE = 10 * 1024 * 1024 # 10 MB in bytes.
	
	_MOTD = """Scalr image 
%(dist_name)s %(dist_version)s %(bits)d-bit
Role: %(role_name)s
Bundled: %(bundle_date)s
"""
	_WALL_MESSAGE = "Server is going to rebundle. Please logout from terminal"
	
	_NUM_UPLOAD_THREADS = 4
	_MAX_UPLOAD_ATTEMPTS = 5
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = bus.platform
		
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
		return message.name == Messages.REBUNDLE and platform == "ec2"	
	

	def on_Rebundle(self, message):
		try:
			image_file = image_mpoint = None
			
			role_name = message.role_name.encode("ascii")
			
			self._before_rebundle(role_name)			
			bus.fire("before_rebundle", role_name=role_name)
			
			aws_account_id = self._platform.get_account_id()
			avail_zone = self._platform.get_avail_zone()
			region = avail_zone[0:2]
			prefix = role_name + "-" + time.strftime("%Y%m%d%H%I")
			cert, pk = self._platform.get_cert_pk()
			ec2_cert = self._platform.get_ec2_cert()
			bucket = "scalr2-images-%s-%s" % (region, aws_account_id)		
			
			# Create exclude directories list
			excludes = message.excludes.encode("ascii").split(":") \
					if message.body.has_key("excludes") and message.excludes else []
			self._logger.debug("Excludes %s", ":".join(excludes))

			# Bundle volume
			image_file, image_mpoint = self._bundle_vol(prefix=prefix, destination="/mnt", excludes=excludes)
			# Execute pre-bundle routines. cleanup files, patch files, etc.
			self._cleanup_image(image_mpoint, role_name=role_name)

			#image_file = "/mnt/scalarizr-debian-1274093259"
			# Bundle image
			manifest_path, manifest = self._bundle_image(
					prefix, image_file, aws_account_id, "/mnt", pk, cert, ec2_cert)
			# Upload image to S3
			s3_manifest_path = self._upload_image(bucket, manifest_path, manifest, region=region)
			# Register image on EC2
			ami_id = self._register_image(s3_manifest_path)
			
			# Send message to Scalr
			self._send_message(Messages.REBUNDLE_RESULT, dict(
				status = "ok",
				snapshot_id = ami_id,
				bundle_task_id = message.bundle_task_id															
			))
			
			# Fire 'rebundle'
			bus.fire("rebundle", role_name=role_name, snapshot_id=ami_id)
			
			optparser = bus.optparser
			if optparser.values.run_import:
				print "Rebundle complete"
			
		except (Exception, BaseException), e:
			self._logger.error("Rebundle failed. %s", e)
			self._logger.exception(e)
			
			# Send message to Scalr
			self._send_message(Messages.REBUNDLE_RESULT, dict(
				status = "error",
				last_error = e.message,
				bundle_task_id = message.bundle_task_id
			))		
			
			# Fire 'rebundle_error'
			bus.fire("rebundle_error", role_name=role_name, last_error=e.message)
			
			optparser = bus.optparser
			if optparser.values.run_import:
				print "Rebundle failed. %s" % (e.message,)
			
		finally:
			try:
				if image_file or image_mpoint:
					self._cleanup(image_file, image_mpoint)
			except (Exception, BaseException), e2:
				self._logger.exception(e2)
				pass
		
		
	def _before_rebundle(self, role_name):
		# Send wall message before rebundling. So console users can run away
		system("wall \"%s\"" % [self._WALL_MESSAGE])


	def _bundle_vol(self, prefix="", volume="/", destination=None, 
				size=None, excludes=None):
		try:
			self._logger.info("Bundling volume '%s'", volume)
			
			self._logger.debug("Checking that user is root")
			if not self._is_super_user():
				raise HandlerError("You need to be root to run rebundle")
			self._logger.debug("User check success")
			
			image_file = destination + "/" + prefix
			if size is None:
				size = LoopbackImage.MAX_IMAGE_SIZE	
			
			self._logger.info("Creating directory exclude list")
			# Create list of directories to exclude from the image
			if excludes is None:
				excludes = []
			
			# Exclude mounted non-local filesystems if they are under the volume root
			mtab = fstool.Mtab()
			excludes += list(entry.mpoint
					for entry in mtab.list_entries()  
					if entry.fstype in fstool.Mtab.LOCAL_FS_TYPES)
			
			# Exclude the image file if it is under the volume root.
			if image_file.startswith(volume):
				excludes.append(image_file)
			
			# Unique list
			excludes = list(set(excludes))
			self._logger.debug("Exclude list: " + str(excludes))		
			
			# Create image from volume
			self._logger.info("Creating loopback image device")
			image = LoopbackImage(volume, image_file, LoopbackImage.MAX_IMAGE_SIZE, excludes)
			image_mpoint = image.make()
			
			self._logger.info("Volume bundle complete!")
			return image_file, image_mpoint
		except (Exception, BaseException), e:
			self._logger.error("Cannot bundle volume. %s", e)
			raise
	
	def _cleanup_image(self, image_mpoint, role_name=None):
		# Create message of the day
		self._create_motd(image_mpoint, role_name)
		
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
						"root/.mysql_history", "root/.history"):
			filename = os.path.join(image_mpoint, filename)
			if os.path.exists(filename):
				os.remove(filename)
		
		
		# Cleanup scalarizr private data
		etc_path = os.path.join(image_mpoint, bus.etc_path[1:])
		shutil.rmtree(os.path.join(etc_path, "private.d"))
		#os.makedirs(os.path.join(etc_path, "private.d/keys"))
		
		bus.fire("rebundle_cleanup_image", image_mpoint=image_mpoint)

	
	def _create_motd(self, image_mpoint, role_name=None):
		# Create message of the day
		motd_filename = os.path.join(image_mpoint, "etc/motd.tail" if disttool.is_debian_based() else "etc/motd")
		if os.path.exists(motd_filename): 
			motd_file = None		
			try:
				dist = disttool.linux_dist()
				motd_file = open(motd_filename, "w")
				motd_file.write(self._MOTD % dict(
					dist_name = dist[0],
					dist_version = dist[1],
					bits = 64 if disttool.uname()[4] == "x86_64" else 32,
					role_name = role_name,
					bundle_date = datetime.today().strftime("%Y-%m-%d %H:%M")
				))
			except OSError, e:
				self._logger.warning("Cannot patch motd file '%s'. %s", motd_filename, str(e))
			finally:
				if motd_file:
					motd_file.close()
		else:
			self._logger.warning("motd file doesn't exists on expected location '%s'", motd_filename)
						
		
	def _bundle_image(self, name, image_file, user, destination, user_private_key_string, 
					user_cert_string, ec2_cert_string):
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
			#self._logger.info("Load ")
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
			key = hexlify(Rand.rand_bytes(16))
			iv = hexlify(Rand.rand_bytes(8))
	
	
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
			
			# digest = "88935fe66e78ce819789dc4a4ef6461f72db6342"
	
			# Split the bundled AMI. 
			# Splitting is not done as part of the compress, encrypt digest
			# stream, so that the filenames of the parts can be easily
			# tracked. The alternative is to create a dedicated output
			# directory, but this leaves the user less choice.
			part_names = filetool.split(bundled_file_path, name, self._IMAGE_CHUNK_SIZE, destination)
			
			# Sum the parts file sizes to get the encrypted file size.
			self._logger.info("Sum the parts file sizes to get the encrypted file size")
			bundled_size = 0
			for part_name in part_names:
				bundled_size += os.path.getsize(os.path.join(destination, part_name))
	
			
			# Encrypt key and iv.
			self._logger.debug("Encrypting keys")
			padding = RSA.pkcs1_padding
			user_encrypted_key = hexlify(user_public_key.get_rsa().public_encrypt(key, padding))
			ec2_encrypted_key = hexlify(ec2_public_key.get_rsa().public_encrypt(key, padding))
			user_encrypted_iv = hexlify(user_public_key.get_rsa().public_encrypt(iv, padding))
			ec2_encrypted_iv = hexlify(ec2_public_key.get_rsa().public_encrypt(iv, padding))
			
			# Digest parts.		
			parts = self._digest_parts(part_names, destination)
			
			arch = disttool.uname()[4]
			if re.search("^i\d86$", arch):
				arch = "i386"
			
			# Create bundle manifest
			manifest = Manifest(
				name=name,
				user=user, 
				arch=arch, 
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
				block_device_mapping=self._platform.get_block_device_mapping()
			)
			manifest.save(manifest_file)
			
			self._logger.info("Image bundle complete!")
			return manifest_file, manifest
		except (Exception, BaseException), e:
			self._logger.error("Cannot bundle image. %s", e)
			raise
	

	def _digest_parts(self, part_names, destination):
		self._logger.info("Generating digests for each part")
		part_digests = []
		for part_name in part_names:
			part_filename = os.path.join(destination, part_name)
			f = None
			try:
				f = open(part_filename)
				digest = EVP.MessageDigest("sha1")
				part_digests.append((part_name, hexlify(cryptotool.digest_file(digest, f)))) 
			except Exception, BaseException:
				self._logger.error("Cannot generate digest for part '%s'", part_name)
				raise
			finally:
				if f is not None:
					f.close()
		return part_digests
		
	def _cleanup(self, image_file, image_mpoint):
		self._logger.debug("Cleanup after bundle")
		
		mtab = fstool.Mtab()
		if mtab.contains(mpoint=image_mpoint):
			self._logger.info("Unmounting '%s'", image_mpoint)				
			system("umount -d " + image_mpoint)
			os.rmdir(image_mpoint)
			
		for path in glob.glob(image_file + "*"):
			try:
				if os.path.isdir(path):
					shutil.rmtree(path, ignore_errors=True)
				else:
					os.remove(path)
			except (OSError, IOError), e:
				self._logger.error("Error during cleanup. %s", e)

		
	def _is_super_user(self):
		out = system("id -u")[0]
		return out.strip() == "0"
	
	def _upload_image(self, bucket_name, manifest_path, manifest, acl="aws-exec-read", region="US"):
		try:
			self._logger.info("Uploading bundle")
			s3_conn = self._platform.new_s3_conn()
			
			# Create bucket
			bucket = None
			location = region.upper()
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
					bucket = s3_conn.create_bucket(bucket_name, 
							location=Location.DEFAULT if location == "US" else location,
							policy=acl)
					
			except BotoServerError, e:
				raise BaseException("Cannot get bucket '%s'. %s" % (bucket_name, e.reason))
			
			# Create files queue
			self._logger.info("Enqueue files to upload")
			manifest_dir = os.path.dirname(manifest_path)
			queue = Queue()
			queue.put((manifest_path, 0))
			for part in manifest.parts:
				queue.put((os.path.join(manifest_dir, part[0]), 0))
			
			# Start uploaders
			self._logger.info("Start uploading with %d threads", self._NUM_UPLOAD_THREADS)
			failed_files = []
			failed_files_lock = Lock()
			uploaders = []
			for n in range(self._NUM_UPLOAD_THREADS):
				uploader = Thread(name="Uploader-%s" % n, target=self._uploader, 
						args=(queue, s3_conn, bucket, acl, failed_files, failed_files_lock))
				self._logger.debug("Starting uploader '%s'", uploader.getName())
				uploader.start()
				uploaders.append(uploader)
			
			# Wait for uploaders
			self._logger.debug("Wait for uploaders")
			for uploader in uploaders:
				uploader.join()
				self._logger.debug("Uploader '%s' finished", uploader.getName())
				
			if failed_files:
				raise BaseException("Cannot upload several files. %s" % [", ".join(failed_files)])
			
			self._logger.info("Upload complete!")
			return os.path.join(bucket_name, os.path.basename(manifest_path))
			
		except (Exception, BaseException), e:
			self._logger.error("Cannot upload image. %s", e)
			raise



	def _uploader(self, queue, s3_conn, bucket, acl, failed_files, failed_files_lock):
		"""
		@param queue: files queue
		@param s3_conn: S3 connection
		@param bucket: S3 bucket object
		@param acl: file S3 acl
		@param failed_files: list of files that failed to upload
		@param failed_files_lock: Lock object to synchronize access to `failed_files`
		"""
		self._logger.debug("queue: %s, bucket: %s", queue, bucket)
		try:
			while 1:
				filename, upload_attempts = queue.get(False)
				try:
					self._logger.info("Uploading '%s' to S3 bucket '%s'", filename, bucket.name)
					key = Key(bucket)
					key.name = os.path.basename(filename)
					file = open(filename, "rb")
					key.set_contents_from_file(file, policy=acl)
				except (BotoServerError, OSError), e:
					self._logger.error("Cannot upload '%s'. %s", filename, e)
					if upload_attempts < self._MAX_UPLOAD_ATTEMPTS:
						self._logger.info("File '%s' will be uploaded within the next attempt", filename)
						upload_attempts += 1
						queue.put((filename, upload_attempts))
					else:
						try:
							failed_files_lock.acquire()
							failed_files.append(filename)
						finally:
							failed_files_lock.release()
		except Empty:
			return
	
	
	def _register_image(self, s3_manifest_path):
		try:
			self._logger.info("Registering image '%s'", s3_manifest_path)
			ec2_conn = self._platform.new_ec2_conn()
			
			# TODO: describe this strange bug in boto when istead of `ImageLocation` param `Location` is sent
			#ami_id = ec2_conn.register_image(None, None, image_location=s3_manifest_path)
			rs = ec2_conn.get_object('RegisterImage', {"ImageLocation" : s3_manifest_path}, ResultSet)
			ami_id = getattr(rs, 'imageId', None)
			
			self._logger.info("Registration complete!")
			return ami_id
		except (BaseException, Exception), e:
			self._logger.error("Cannot register image on EC2. %s", e)
			raise
		

if disttool.is_linux():
	class LinuxLoopbackImage:
		"""
		This class encapsulate functionality to create an file loopback image
		from a volume. The image is created using dd. Sub-directories of the 
		volume, including mounts of local filesystems, are copied to the image. 
		Symbolic links are preserved. 	
		"""
		
		MAX_IMAGE_SIZE = 10*1024
		
		_volume = None
		_image_file = None
		_image_size = None	
		_image_mpoint = "/mnt/img-mnt"
		_excludes = None	
		_logger = None
		
		_mtab = None
		
		def __init__(self, volume, image_file, image_size, excludes):
			self._logger = logging.getLogger(__name__)
			
			self._volume = volume
			self._image_file = image_file
			self._image_size = image_size
			
			self._excludes = excludes
			if self._image_mpoint.startswith(volume):
				self._excludes.append(self._image_mpoint)
			"""
			self._excludes.append("/mnt")
			self._excludes.append("/sys")
			self._excludes.append("/proc")
			"""
		
			self._mtab = fstool.Mtab()
		
		def make(self):
			self._logger.info("Copying %s into the image file %s", self._volume, self._image_file)
			self._logger.info("Exclude list: %s", ":".join(self._excludes))
	
			self._create_image_file()
			self._format_image()
			system("sync")  # Flush so newly formatted filesystem is ready to mount.
			self._mount_image()
			self._make_special_dirs()
			self._copy_rec(self._volume, self._image_mpoint)
			return self._image_mpoint
			
		def _create_image_file(self):
			self._logger.info("Create image file")
			system("dd if=/dev/zero of=" + self._image_file + " bs=1M count=1 seek=" + str(self._image_size))
		
		def _format_image(self):
			self._logger.info("Format image file")
			system("/sbin/mkfs.ext3 -F " + self._image_file)
			system("/sbin/tune2fs -i 0 " + self._image_file)
		
		def _mount_image(self):
			"""
			Mount the image file as a loopback device. The mount point is created
			if necessary.		
			"""
			self._logger.info("Mount image file")
			if self._mtab.contains(mpoint=self._image_mpoint):
				raise HandlerError("Image already mounted")
			fstool.mount(self._image_file, self._image_mpoint, ["-o loop"])
		
		def _make_special_dirs(self):
			self._logger.info("Make special directories")
			
			mtab = fstool.Mtab()
			special_dirs = list(entry.mpoint
					for entry in mtab.list_entries()  
					if entry.fstype in fstool.Mtab.LOCAL_FS_TYPES)
			special_dirs.extend(["/mnt", "/proc", "/sys", "/dev"])
			
			for dir in special_dirs:
				spec_dir = self._image_mpoint + dir
				if not os.path.exists(spec_dir):
					self._logger.debug("Create spec dir %s", spec_dir)
					os.makedirs(spec_dir)
			
			
			# MAKEDEV is incredibly variable across distros, so use mknod directly.
			dev_dir = self._image_mpoint + "/dev"			
			system("mknod " + dev_dir + "/null c 1 3")
			system("mknod " + dev_dir + "/zero c 1 5")
			system("mknod " + dev_dir + "/tty c 5 0")
			system("mknod " + dev_dir + "/console c 5 1")
			system("ln -s null " + dev_dir +"/X0R")		
		
		def _copy_rec(self, source, dest, xattr=True):
			self._logger.info("Copy volume to image file")
			rsync = filetool.Rsync()
			#rsync.archive().times().sparse().links().quietly()
			rsync.archive().times().sparse().links().verbose()
			if xattr:
				rsync.xattributes()
			rsync.exclude(self._excludes)
			rsync.source(os.path.join(source, "/*")).dest(dest)
			exitcode = system(str(rsync))[2]
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
		
		"""
		def _update_fstab(self):
			pass
		"""
				
	LoopbackImage = LinuxLoopbackImage
	
elif disttool.is_sun():
	class SolarisLoopbakImage:
		
		MAX_IMAGE_SIZE = 10*1024
		
		"""
		@todo: Solaris support
		"""
		pass
	
	LoopbackImage = SolarisLoopbakImage


			


	


class Manifest:
	
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
			for virtual, device in self.block_device_mapping.items():
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

