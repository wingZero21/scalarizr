'''
Created on Mar 11, 2010

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.core.handlers import Handler
from scalarizr.platform import PlatformError
from scalarizr.platform.ec2 import Mtab, AwsPlatformOptions
from scalarizr.messaging import Messages
import subprocess
import logging
import time
import os


class Ec2RebundleHandler(Handler):
	_logger = None
	
	_platform = None
	"""
	@ivar scalarizr.platform.ec2.AwsPlatform: 
	"""
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = Bus()[BusEntries.PLATFORM]
	
	def on_Rebundle(self, message):
	
		aws_account_id = self._platform.get_config_option(AwsPlatformOptions.ACCOUNT_ID)
		avail_zone = self._platform.get_avail_zone()
		region = avail_zone[0:2]
		prefix = message.body["role_name"] + "-" + str(int(time.time()))
		cert, pk = self._platform.get_cert_pk()
		
		# Create exclude directories list
		excludes = message.body["excludes"].split(":") \
				if message.body.has_key("excludes") else []
		excludes += ["/mnt"]		
		
		try:
			self._bundle_vol(
				prefix=prefix, 
				destination="/mnt", 
				excludes=excludes, 
				cert=cert, 
				pk=pk, 
				account_id=aws_account_id, 
				kernel_id=self._platform.get_kernel_id(), 
				ramdisk_id=self._platform.get_ramdisk_id(), 
				region=region, 
				avail_zone=avail_zone
			)
		except (BaseException, Exception), e:
			self._logger.error("Cannot bundle image. Error: %s", str(e))
			raise
			
		bucket = "scalr-images-%s-%s" % (region, aws_account_id)
		
		
		pass

	def _bundle_vol(self, prefix="", volume="/", destination=None, 
				size=LoopbackImage.MAX_IMAGE_SIZE, excludes=[], 
				cert=None, pk=None, account_id=None, 
				kernel_id="", ramdisk_id="", region=None, avail_zone=None):
		
		if not self._is_super_user():
			raise PlatformError("You need to be root to run rebundle")
		
		image_file = destination + "/" + prefix	
		
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
		
		
		
		
		pass
	
	def _exec(self, cmd):
		return subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0] 
	
	def _is_super_user(self):
		return self._exec(["id", "-u"]).strip() == "0"
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE and platform == "ec2"
	
	
class LoopbackImage:
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
		self._logger.info("Exclude list: %s", self._excludes.join(":"))

		try:
			self._create_image_file()
			self._format_image()
			os.system("sync")  # Flush so newly formatted filesystem is ready to mount.
			self._mount_image()
			self._make_special_dirs()
			self._copy_rec(self._volume, self._image_mpoint)
		except:
			self._cleanup()
		
	def _create_image_file(self):
		os.system("dd if=/dev/zero of=" + self._image_file + " bs=1M count=1 seek=" + self._image_size)
	
	def _format_image(self):
		os.system("/sbin/mkfs.ext3 -F " + self._image_file)
		os.system("/sbin/tune2fs -i 0 " + self._image_file)
	
	def _mount_image(self):
		"""
		Mount the image file as a loopback device. The mount point is created
		if necessary.		
		"""
		if not os.path.exists(self._image_mpoint):
			os.makedirs(self._image_mpoint)
		if self._is_mounted(self._image_mpoint):
			raise PlatformError("Image already mounted")
		os.system("mount -o loop " + self._image_file + " " + self._image_mpoint)
	
	def _make_special_dirs(self):
		# Make /proc /sys /mnt
		os.makedirs(self._image_mpoint + "/mnt")
		os.makedirs(self._image_mpoint + "/proc")
		os.makedirs(self._image_mpoint + "/sys")
		
		# Make device nodes.
		dev_dir = self._image_mpoint + "/dev"
		os.makedirs(dev_dir)
		# MAKEDEV is incredibly variable across distros, so use mknod directly.
		os.system("mknod " + dev_dir +"/null c 1 3")
		os.system("mknod " + dev_dir +"/zero c 1 5")
		os.system("mknod " + dev_dir +"/tty c 5 0")
		os.system("mknod " + dev_dir +"/console c 5 1")
		os.system("ln -s null " + dev_dir +"/X0R")		
	
	def _copy_rec(self, source, dest):
		pass
	
	"""
	def _update_fstab(self):
		pass
	"""
	
	def _cleanup (self):
		pass
		
		
	def _is_mounted(self, mpoint):
		mtab = Mtab()
		for entry in mtab.list_entries():
			if entry.mpoint == mpoint:
				return True
		return False
		
	def _unmount(self, mpoint):
		if not self._is_mounted(mpoint):
			os.system("umount -d " + mpoint)