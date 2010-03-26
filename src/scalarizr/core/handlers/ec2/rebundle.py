'''
Created on Mar 11, 2010

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.core.handlers import Handler
from scalarizr.platform import PlatformError
from scalarizr.messaging import Messages
from scalarizr.util.disttool import DistTool
from scalarizr.util import system, CryptoUtil
import logging
import time
import os
import re
from M2Crypto import X509, EVP, Rand, RSA
from binascii import hexlify
from xml.dom.minidom import Document
from datetime import datetime
from threading import Thread
from Queue import Queue, Empty
import math
from boto.s3 import Key
from boto.s3.connection import Location
from boto.resultset import ResultSet
from boto.exception import BotoServerError


def get_handlers ():
	return [Ec2RebundleHandler()]


BUNDLER_NAME = "scalarizr"
BUNDLER_VERSION = "0.9"
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
	
	_NUM_UPLOAD_THREADS = 4
	_MAX_UPLOAD_ATTEMPTS = 5
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = Bus()[BusEntries.PLATFORM]
		self._msg_service = Bus()[BusEntries.MESSAGE_SERVICE]
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE and platform == "ec2"	
	
	def on_Rebundle(self, message):
		
		
		# TODO: Truncate log files
		
		# Clear user activity history
		for filename in ("/root/.bash_history", "/root/.lesshst", "/root/.viminfo", 
						"/root/.mysql_history", "/root/.history"):
			if os.path.exists(filename):
				os.remove(filename)
		
		
		# Create message of the day
		dt = DistTool()
		motd_filename = "/etc/motd.tail" if dt.is_debian_based() else "/etc/motd"
		motd_file = None		
		try:
			dist = dt.linux_dist()
			motd_file = open(motd_filename, "w")
			motd_file.write(self._MOTD % dict(
				dist_name = dist[0],
				dist_version = dist[1],
				bits = 64 if dt.uname()[4] == "x86_64" else 32,
				role_name = message.role_name,
				bundle_date = datetime.today().strftime("%Y-%m-%d %H:%M")
			))
		except OSError, e:
			self._logger.warning("Cannot patch motd file '%s'. %s", motd_filename, str(e))
		finally:
			if motd_file:
				motd_file.close()
		
		
		aws_account_id = self._platform.get_account_id()
		avail_zone = self._platform.get_avail_zone()
		region = avail_zone[0:2]
		prefix = message.role_name + "-" + str(int(time.time()))
		cert, pk = self._platform.get_cert_pk()
		ec2_cert = self._platform.get_ec2_cert()
		bucket = "scalr2-images-%s-%s" % (region, aws_account_id)		
		
		# Create exclude directories list
		excludes = message["excludes"].split(":") \
				if message.body.has_key("excludes") else []
		base_path = Bus()[BusEntries.BASE_PATH]
		excludes += ["/mnt", base_path + "/etc/.*"]		
		
		# Bundle volume
		image_file = self._bundle_vol(prefix=prefix, destination="/mnt", excludes=excludes)
		# Bundle image
		manifest_path, manifest = self._bundle_image(
				prefix, image_file, aws_account_id, "/mnt", pk, cert, ec2_cert)
		# Upload image to S3 
		s3_manifest_path = self._upload_image(bucket, manifest_path, manifest, region=region)
		# Register image on EC2
		self._register_image(s3_manifest_path)


	def _bundle_vol(self, prefix="", volume="/", destination=None, 
				size=None, excludes=[]):
		try:
			self._logger.info("Bundling volume '%s'", volume)
			
			self._logger.debug("Checking that user is root")
			if not self._is_super_user():
				raise PlatformError("You need to be root to run rebundle")
			self._logger.debug("User check success")
			
			image_file = destination + "/" + prefix
			if size is None:
				size = LoopbackImage.MAX_IMAGE_SIZE	
			
			self._logger.info("Creating directory exclude list")
			# Create list of directories to exclude from the image
			if excludes is None:
				excludes = []
			
			# Exclude mounted non-local filesystems if they are under the volume root
			mtab = Mtab()
			excludes += list(entry.mpoint for entry in mtab.list_entries()  
					if entry.fstype in Mtab.LOCAL_FS_TYPES)
			
			# Exclude the image file if it is under the volume root.
			if image_file.startswith(volume):
				excludes.append(image_file)
			
			# Unique list
			excludes = list(set(excludes))
			self._logger.debug("Exclude list: " + str(excludes))		
			
			# Create image from volume
			self._logger.info("Creating loopback image device")
			image = LoopbackImage(volume, image_file, LoopbackImage.MAX_IMAGE_SIZE, excludes)
			image.make()
			
			self._logger.info("Volume bundle complete!")
			return image_file
		except (Exception, BaseException), e:
			self._logger.error("Cannot bundle volume. %s", e)
			raise
		
		
	def _bundle_image(self, name, image_file, user, destination, user_private_key_string, 
					user_cert_string, ec2_cert_string):
		try:
			self._logger.info("Bundling image")
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
			_dt = DistTool()
			openssl = "/usr/sfw/bin/openssl" if _dt.is_sun() else "openssl"
			tar = Tar()
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
				#os.remove(digest_file)
				pass

			#digest = "1f75937c890092bb8046af3cde0b575b7e5728f9"
	
			# Split the bundled AMI. 
			# Splitting is not done as part of the compress, encrypt digest
			# stream, so that the filenames of the parts can be easily
			# tracked. The alternative is to create a dedicated output
			# directory, but this leaves the user less choice.
			part_names = FileUtil.split(bundled_file_path, name, self._IMAGE_CHUNK_SIZE, destination)
			
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
			
			arch = DistTool().uname()[4]
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
		cu = CryptoUtil()
		part_digests = []
		for part_name in part_names:
			part_filename = os.path.join(destination, part_name)
			f = None
			try:
				f = open(part_filename)
				digest = EVP.MessageDigest("sha1")
				part_digests.append((part_name, hexlify(cu.digest_file(digest, f)))) 
			except Exception, BaseException:
				self._logger.error("Cannot generate digest for part '%s'", part_name)
				raise
			finally:
				if f is not None:
					f.close()
		return part_digests
		
		
	def _is_super_user(self):
		out = system("id -u")[0]
		return out.strip() == "0"
	
	def _upload_image(self, bucket_name, manifest_path, manifest, acl="aws-exec-read", region="US"):
		try:
			self._logger.info("Uploading bundle")
			s3_conn = self._platform.get_s3_conn()
			
			# Create bucket
			bucket = None
			location = region.upper()
			try:
				self._logger.info("Creating bucket '%s'", bucket_name)
				bucket = s3_conn.create_bucket(bucket_name, 
						location=Location.DEFAULT if location == "US" else location,
						policy=acl)
			except BotoServerError, e:
				self._logger.error("Cannot create bucket '%s'. %s", bucket_name, e.reason)
				raise e
			
			# Create files queue
			self._logger.info("Enqueue files to upload")
			dir = os.path.dirname(manifest_path)
			queue = Queue()
			queue.put((manifest_path, 0))
			for part in manifest.parts:
				queue.put((os.path.join(dir, part[0]), 0))
			
			# Start uploaders
			self._logger.info("Start uploading with %d threads", self._NUM_UPLOAD_THREADS)
			uploaders = []
			for n in range(self._NUM_UPLOAD_THREADS):
				uploader = Thread(name="Uploader-%s" % n, target=self._uploader, args=(queue, s3_conn, bucket, acl))
				self._logger.debug("Starting uploader '%s'", uploader.getName())
				uploader.start()
				uploaders.append(uploader)
			
			# Wait for uploaders
			self._logger.debug("Wait for uploaders")
			for uploader in uploaders:
				uploader.join()
				self._logger.debug("Uploader '%s' finished", uploader.getName())
	
			self._logger.info("Upload complete!")
			return os.path.join(bucket_name, os.path.basename(manifest_path))
			
		except (Exception, BaseException), e:
			self._logger.error("Cannot upload image. %s", e)
			raise


	def _uploader(self, queue, s3_conn, bucket, acl):
		try:
			while 1:
				msg = queue.get(False)
				try:
					self._logger.info("Uploading '%s' to S3 bucket '%s'", msg[0], bucket.name)
					key = Key(bucket)
					key.name = os.path.basename(msg[0])
					file = open(msg[0], "rb")
					key.set_contents_from_file(file, policy=acl)
				except (BotoServerError, OSError), e:
					self._logger.error("Cannot upload '%s'. %s", msg[0], e)
					if isinstance(e, BotoServerError) and msg[1] < self._MAX_UPLOAD_ATTEMPTS:
						self._logger.info("File '%s' will be uploaded within the next attempt", msg[0])
						msg[1] += 1
						queue.put(msg)
					# TODO: collect failed files and report them at the end						
		except Empty:
			return
	
	
	def _register_image(self, s3_manifest_path):
		try:
			self._logger.info("Registering image '%s'", s3_manifest_path)
			ec2_conn = self._platform.get_ec2_conn()
			
			# TODO: describe this strange bug in boto when istead of `ImageLocation` param `Location` is sent
			#ami_id = ec2_conn.register_image(None, None, image_location=s3_manifest_path)
			rs = ec2_conn.get_object('RegisterImage', {"ImageLocation" : s3_manifest_path}, ResultSet)
			ami_id = getattr(rs, 'imageId', None)
			
			self._logger.info("Registration complete!")
			return ami_id
		except (BaseException, Exception), e:
			self._logger.error("Cannot register image on EC2. %s", e)
			raise
	
class Fstab:
	"""
	Wrapper over /etc/fstab
	"""
	LOCATION = None
	_entries = None
	_filename = None
	_re = None
	
	def __init__(self, filename=None):
		self._filename = filename if not filename is None else Mtab.LOCATION
		self._entries = None
		self._re = re.compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+).*$")
		
	def list_entries(self, rescan=False):
		if not self._entries or rescan:
			self._entries = []
			f = open(self._filename, "r")
			for line in f:
				if line[0:1] == "#":
					continue
				m = self._re.match(line)
				if m:
					self._entries.append(_TabEntry(
						m.group(1), m.group(2), m.group(3), m.group(4), line.strip()
					))
			f.close()
			
		return list(self._entries)

class Mtab(Fstab):
	"""
	Wrapper over /etc/mtab
	"""
	LOCAL_FS_TYPES = None	
		
class _TabEntry(object):
	device = None
	mpoint = None
	fstype = None
	options = None	
	value = None
	
	def __init__(self, device, mpoint, fstype, options, value):
		self.device = device
		self.mpoint = mpoint
		self.fstype = fstype
		self.options = options		
		self.value = value

		
_dt = DistTool()
if _dt.is_linux():
	Fstab.LOCATION = "/etc/fstab"	
	Mtab.LOCATION = "/etc/mtab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs')
	
elif _dt.is_sun():
	Fstab.LOCATION = "/etc/vfstab"	
	Mtab.LOCATION = "/etc/mnttab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 
		'ufs', 'sharefs', 'dev', 'devfs', 'ctfs', 'mntfs',
		'proc', 'lofs',   'objfs', 'fd', 'autofs')


if _dt.is_linux():
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
		
		def __init__(self, volume, image_file, image_size, excludes):
			self._logger = logging.getLogger(__name__)
			
			self._volume = volume
			self._image_file = image_file
			self._image_size = image_size
			self._excludes = excludes
	
			if self._image_mpoint.startswith(volume):
				self._excludes.append(self._image_mpoint)
		
		
		def make(self):
			self._logger.info("Copying %s into the image file %s...", self._volume, self._image_file)
			self._logger.info("Exclude list: %s", ":".join(self._excludes))
	
			try:
				self._create_image_file()
				self._format_image()
				system("sync")  # Flush so newly formatted filesystem is ready to mount.
				self._mount_image()
				self._make_special_dirs()
				self._copy_rec(self._volume, self._image_mpoint)
			except:
				self._cleanup()
				raise
			
		def _create_image_file(self):
			self._logger.debug("Create image file")
			system("dd if=/dev/zero of=" + self._image_file + " bs=1M count=1 seek=" + str(self._image_size))
		
		def _format_image(self):
			self._logger.debug("Format image file")
			system("/sbin/mkfs.ext3 -F " + self._image_file)
			system("/sbin/tune2fs -i 0 " + self._image_file)
		
		def _mount_image(self):
			"""
			Mount the image file as a loopback device. The mount point is created
			if necessary.		
			"""
			self._logger.debug("Mount image file")
			if not os.path.exists(self._image_mpoint):
				os.makedirs(self._image_mpoint)
			if self._is_mounted(self._image_mpoint):
				raise PlatformError("Image already mounted")
			system("mount -o loop " + self._image_file + " " + self._image_mpoint)
		
		def _make_special_dirs(self):
			self._logger.debug("Make special directories")
			# Make /proc /sys /mnt
			os.makedirs(self._image_mpoint + "/mnt")
			os.makedirs(self._image_mpoint + "/proc")
			os.makedirs(self._image_mpoint + "/sys")
			
			# Make device nodes.
			dev_dir = self._image_mpoint + "/dev"
			os.makedirs(dev_dir)
			# MAKEDEV is incredibly variable across distros, so use mknod directly.
			system("mknod " + dev_dir + "/null c 1 3")
			system("mknod " + dev_dir + "/zero c 1 5")
			system("mknod " + dev_dir + "/tty c 5 0")
			system("mknod " + dev_dir + "/console c 5 1")
			system("ln -s null " + dev_dir +"/X0R")		
		
		def _copy_rec(self, source, dest, xattr=True):
			self._logger.debug("Copy volume to image file")
			rsync = Rsync()
			rsync.archive().times().sparse().links().quietly()
			if xattr:
				rsync.xattributes()
			rsync.exclude(self._excludes)
			rsync.source(os.path.join(source, "/*")).dest(dest)
			exitcode = system(str(rsync))[2]
			if exitcode == 23 and Rsync.usable():
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
		
		def _cleanup (self):
			self._unmount(self._image_mpoint)
			
		def _is_mounted(self, mpoint):
			self._logger.debug("Checking that '%s' is mounted", mpoint)
			mtab = Mtab()
			for entry in mtab.list_entries():
				if entry.mpoint == mpoint:
					return True
			return False
			
		def _unmount(self, mpoint):
			if self._is_mounted(mpoint):
				self._logger.debug("Unmounting '%s'", mpoint)				
				system("umount -d " + mpoint)
				os.rmdir(mpoint)
				
	LoopbackImage = LinuxLoopbackImage
	
elif _dt.is_sun():
	class SolarisLoopbakImage:
		
		MAX_IMAGE_SIZE = 10*1024
		
		"""
		@todo: Solaris support
		"""
		pass
	
	LoopbackImage = SolarisLoopbakImage


			
class Rsync(object):
	"""
	Wrapper for rsync
	"""
	
	EXECUTABLE = "rsync"
	_options = None
	_src = None
	_dst = None
	_executable = None
	_quiet = None
	
	def __init__(self, executable=None):
		self._executable = executable if executable is not None else Rsync.EXECUTABLE
		self._options = []
		self._src = self._dst = None
		self._quiet = False
		
	def archive(self):
		self._options.append('-rlpgoD')
		return self
	
	def times(self):
		self._options.append('-t')
		return self
	
	def recursive(self):
		self._options.append('-r')
		return self
		
	def sparse(self):
		self._options.append('-S')
		return self

	def links(self):
		self._options.append('-l')
		return self
		
	def dereference(self):
		self._options.append('-L')
		return self
	
	def xattributes(self):
		self._options.append('-X')
		return self

	def exclude(self, files):
		for file in files:
			self._options.append("--exclude " + file)
		return self

	def version(self):
		self._options.append("--version")
		return self
		
	def source(self, path):
		self._src = path
		return self
		
	def dest(self, path):
		self._dst = path
		return self
		
	def quietly(self):
		self._quiet = True
		return self
	
	def __str__(self):
		ret = "%(executable)s %(options)s %(src)s %(dst)s %(quiet)s" % dict(
			executable=self._executable,
			options=" ".join(self._options),
			src=self._src,
			dst=self._dst,
			quiet="2>&1 > /dev/null" if self._quiet else ""
		)
		return ret.strip()

	@staticmethod
	def usable():
		"""
		@todo: implement
		"""
		return True


class Tar:
	EXECUTABLE = "/usr/sfw/bin/gtar" if DistTool().is_sun() else "tar"
	
	_executable = None
	_options = None
	_files = None
	
	def __init__(self, executable=None):
		self._executable = executable if executable is not None else self.EXECUTABLE
		self._options = []
		self._files = []
		
	def version(self):
		self._options.append("--version")
		return self
	
	def verbose(self):
		self._options.append("-v")
		return self
	
	def create(self):
		self._options.append("-c")
		return self
		
	def bzip2(self):
		self._options.append("-j")
		return self

	def diff(self):
		self._options.append("-d")
		return self
		
	def gzip(self):
		self._options.append("-z")
		return self

	def extract(self):
		self._options.append("-x")
		return self

	def update(self):
		self._options.append("-u")
		return self

	def sparse(self):
		self._options.append("-S")
		return self

	def dereference(self):
		self._options.append("-h")
		return self

	def archive(self, filename):
		self._options.append("-f " + filename if filename is not None else "-")
		return self
	
	def chdir(self, dir):
		self._options.append("-C " + dir)
		return self
	
	def add(self, filename, dir=None):
		item = filename if dir is None else "-C "+dir+" "+filename
		self._files.append(item)
		return self
	
	def __str__(self):
		ret = "%(executable)s %(options)s %(files)s" % dict(
			executable=self._executable,
			options=" ".join(self._options),
			files=" ".join(self._files)
		)
		return ret.strip()
	

class FileUtil:
	BUFFER_SIZE = 1024 * 1024	# Buffer size in bytes.
	PART_SUFFIX = '.part.'	
	
	@staticmethod
	def split(filename, part_name_prefix, chunk_size, dest_dir):
		logger = logging.getLogger(__name__)
		f = None
		try:
			try:
				f = open(filename, "r")
			except OSError:
				logger.error("Cannot open file to split '%s'", filename)
				raise
			
			# Create the part file upfront to catch any creation/access errors
			# before writing out data.
			num_parts = int(math.ceil(float(os.path.getsize(filename))/chunk_size))
			part_names = []
			logger.info("Splitting file '%s' into %d chunks", filename, num_parts)
			for i in range(num_parts):
				part_name_suffix = FileUtil.PART_SUFFIX + str(i).rjust(2, "0")
				part_name = part_name_prefix + part_name_suffix
				part_names.append(part_name)
				
				part_filename = os.path.join(dest_dir, part_name)
				try:
					FileUtil.touch(part_filename)
				except OSError:
					logger.error("Cannot create part file '%s'", part_filename)
					raise
						
			# Write parts to files.
			for part_name in part_names:
				part_filename = os.path.join(dest_dir, part_name)
				cf = open(part_filename, "w")
				try:
					logger.info("Writing chunk '%s'", part_filename)
					FileUtil._write_chunk(f, cf, chunk_size)
				except OSError:
					logger.error("Cannot write chunk file '%s'", part_filename)
					raise
				
			return part_names
		finally:
			if f is not None:
				f.close()
	
	@staticmethod	
	def _write_chunk(source_file, chunk_file, chunk_size):
		bytes_written = 0  # Bytes written.
		bytes_left = chunk_size	# Bytes left to write in this chunk.
		
		while bytes_left > 0:
			size = FileUtil.BUFFER_SIZE if FileUtil.BUFFER_SIZE < bytes_left else bytes_left
			buf = source_file.read(size)
			chunk_file.write(buf)
			bytes_written += len(buf)
			bytes_left = chunk_size - bytes_written
			if len(buf) < size:
				bytes_left = 0 # EOF
	
	@staticmethod
	def touch(filename):
		open(filename, "w+").close()


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
