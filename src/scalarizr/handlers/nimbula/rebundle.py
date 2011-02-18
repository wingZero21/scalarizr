'''
Created on Feb 15, 2011

@author: spike
'''

from scalarizr.handlers import Handler, HandlerError, RebundleLogHandler
from scalarizr.messaging import Messages
from scalarizr.util.filetool import df, Rsync, Tar, read_file, write_file
from scalarizr.util.fstool import mount, umount
from scalarizr.util import system2, software, cryptotool
from tempfile import mkdtemp
import logging
import os
import re
from scalarizr.util.software import whereis
import pexpect
import shutil

def get_handlers():
	return [NimbulaRebundleHandler()]

class NimbulaRebundleHandler(Handler):	
	
	loop			= None
	root_mpoint		= None
	rebundle_dir	= None
	mounted			= None
	_logger			= None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._log_hdlr = RebundleLogHandler()
		self.nimbula_pass_hash = "$6$rAi9LwiA$/1n3fk2TUU5iLykyxgzHMUPHjOZgkIz4g2dGJpaohmyqPWb5xRj.JqxKfELd.brBivQBh8f5cJL7XEiBRD1ED."


	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE
	
	def on_Rebundle(self, message):
		self._log_hdlr.bundle_task_id = message.bundle_task_id
		self._logger.addHandler(self._log_hdlr)	
		try:
			excludes = ('/dev', '/media', '/mnt', '/proc', '/sys', '/cdrom', '/tmp')
			root_uuid = "97e128e3-a209-4a34-81f7-c35fb9053e25"
			root_size = swap_size = 0
			fs_list = df()
			# Getting root device size 
			for fs in fs_list:
				if fs.mpoint == '/':
					root_size = fs.size
			# Getting swap size
			raw_swap_list = system2('swapon -s', shell = True)[0].splitlines()
			if len(raw_swap_list) > 1:
				swap_size = int(raw_swap_list[1].split()[2])
			
			self.rebundle_dir	= mkdtemp()
			self.tmp_root_dir 	= mkdtemp()
			targz_name 		= message.role_name + '.tar.gz'
			image_name		= message.role_name + '.img'
			image_path		= os.path.join(self.rebundle_dir, image_name)
			
			self._rsync('/', self.tmp_root_dir, excludes=excludes)
			self._make_spec_dirs(self.tmp_root_dir, excludes)
			
			mount('/dev', self.tmp_root_dir + '/dev', options=('--bind',))
			shell = pexpect.spawn('/bin/sh')
			try:
				shell.expect('#')
				shell.sendline('chroot %s' % self.tmp_root_dir)
				shell.expect('#')
				shell.sendline('update-grub -y')
				shell.expect('#')
				shell.sendline('sed -i s/^# kopt=.*$/# kopt=root=UUID=97e128e3-a209-4a34-81f7-c35fb9053e25 ro console=tty1 console=ttyS0/ /boot/grub/menu.lst')
				shell.expect('#')
				shell.sendline('sed -i s/^# defoptions=.*$/# defoptions=/ /boot/grub/menu.lst')
				shell.expect('#')
				shell.sendline('sed -i s/^# groot=.*$/# groot=(hd0,0)/ /boot/grub/menu.lst')
				shell.expect('#')
				shell.sendline('update-grub')
				shell.expect('#')
			finally:
				shell.close()
				
			umount(mpoint=self.tmp_root_dir + '/dev')				
			
			self._logger.debug('Creating image file: %s' % image_path)
			f = open(image_path, 'w')
			f.seek((root_size + swap_size) * 1024 + 63*512 - 1)
			f.write('\0')
			f.close()
			
			start = 62
			part_string = '%s,%s,L,*\n' % (start+1, root_size*2)
			if swap_size:
				start += root_size*2
				part_string += '%s,%s,S\n' % (start+1, swap_size*2)
				
				
			self._logger.debug('Partitioning image.')
			
			out, err, ret_code = system2('sfdisk -uS --force %s' % image_path, stdin=part_string, shell=True, raise_exc=False)
			if ret_code:
				raise HandlerError('Error occured while partitioning image.\n%s' % err)
			
			
			self._logger.debug('Creating device map on image.')		
			out, err, ret_code = system2('kpartx -av %s' % image_path, shell=True, raise_exc=False)
			if ret_code:
				raise HandlerError('Error occured while creating device map.\n%s' % err)
			
			self.loop = re.search('(/dev/loop\d+)', out).group(1)
			root_dev_name = '/dev/mapper/%sp1' % self.loop.split('/')[-1]
			
			
			self._logger.debug('Creating filesystem on root partition.')
			
			out, err, ret_code = system2('mkfs -t ext3 -L root -m 0 -I 128 %s' % root_dev_name, shell=True)
			
			if ret_code:
				raise HandlerError("Can't create filesystem on device %s:\n%s" % (root_dev_name, err))
			if swap_size:
				swap_dev_name = '/dev/mapper/%sp2' % self.loop.split('/')[-1]
				self._logger.debug('Creating filesystem on swap partition.')
				out, err, ret_code = system2('mkswap -L swap %s' % swap_dev_name, shell=True)
				if ret_code:		
					raise HandlerError("Can't create filesystem on device %s:\n%s" % (root_dev_name, err))
			
			
			self._logger.debug('Setting predefined UUID to root device.')
			# Set UUID for root device
			system2('tune2fs -i 0 -U %s %s' % (root_uuid, root_dev_name), shell=True, raise_exc=False)
			
			self.root_mpoint = os.path.join(self.rebundle_dir, 'mnt')
			if not os.path.isdir(self.root_mpoint):
				os.mkdir(self.root_mpoint)
			
			self._logger.debug('Mounting root device to %s.' % self.root_mpoint)			
			mount(root_dev_name, self.root_mpoint)
			self.mounted = True
			
			self._logger.info('Copying / to image.')
			self._rsync(self.tmp_root_dir + os.sep, self.root_mpoint, excludes=excludes)
			self._make_spec_dirs(self.root_mpoint, excludes)
						
			self._logger.info('Creating "scalr" user.')
			shell = pexpect.spawn('/bin/sh')
			try:
				shell.expect('#')
				shell.sendline('chroot %s' % self.root_mpoint)
				shell.expect('#')
				c_count = shell.sendline('useradd scalr -g0')
				shell.expect('#')
				out = shell.before[c_count].strip()
				if out and not "already exists" in out:
					raise HandlerError("Cannot add 'scalr' user: %s" % out)
				
				self._logger.info('Updating password of "scalr" user.')
				scalr_password = cryptotool.keygen(10).strip()
				shell.sendline('passwd scalr')		
				shell.expect('Enter new UNIX password:')
				shell.sendline(scalr_password)
				shell.expect('Retype new UNIX password:')
				shell.sendline(scalr_password)
				shell.expect('passwd: password updated successfully')
			finally:
				shell.close()
				
			self._logger.debug('Unmounting root device.')
						
			umount(root_dev_name)
			self.mounted = False
			
			system2('kpartx -d %s' % image_path, shell=True)
			self.loop = self.root_mpoint = None
			
			grub_path = whereis('grub')
			if not grub_path:
				raise HandlerError("Grub executable was not found.")
			grub_path = self.tmp_root_dir + grub_path[0]
			stdin = 'device (hd0) %s\nroot (hd0,0)\nsetup (hd0)\n' % image_path
			self._logger.info('Installing grub to the image %s' % image_name)			
			system2('%s --batch --no-floppy --device-map=/dev/null' % grub_path, stdin = stdin, shell=True)

			tar = Tar()
			tar.create().gzip().sparse()
			tar.archive(targz_name)
			tar.add(image_name, self.rebundle_dir)

			self._logger.debug('Compressing image')
			system2(str(tar), shell=True)

			msg_data = dict(
				status = "ok",
				#snapshot_id = image.id,
				bundle_task_id = message.bundle_task_id
			)
		
			self._logger.debug("Updating message with OS and software info")
			msg_data.update(software.system_info())
			
			self.send_message(Messages.REBUNDLE_RESULT, msg_data)
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			last_error = str(e)
			
			# Send message to Scalr
			self.send_message(Messages.REBUNDLE_RESULT, dict(
				status = "error",
				last_error = last_error,
				bundle_task_id = message.bundle_task_id
			))
			
		finally:
			self._log_hdlr.bundle_task_id = None
			self._logger.removeHandler(self._log_hdlr)
			
			if self.mounted:
				umount(mpoint=self.root_mpoint)
				self.root_mpoint 	= None
				self.mounted		= False
			if self.loop:
				system2('kpartx -d %s' % image_path, shell=True)
				self.loop = None
			if self.rebundle_dir:
				shutil.rmtree(self.rebundle_dir)
				self.rebundle_dir = None
				
				
	def _make_spec_dirs(self, root_path, excludes):
		for dir in excludes:
			spec_dir = root_path + dir
			if os.path.exists(dir) and not os.path.exists(spec_dir):
				self._logger.debug("Create spec dir %s", dir)
				os.makedirs(spec_dir)
				if dir == '/tmp':
					os.chmod(spec_dir, 01777)
	
	def _rsync(self, src, dst, excludes=None, xattr=True):
		rsync = Rsync()
		rsync.archive().sparse()
		rsync.source(src).dest(dst)
				
		if xattr:
			rsync.xattributes()
		
		if excludes:
			rsync.exclude(excludes)
					
		out, err, ret_code = rsync.execute()
		
		if ret_code == 24 and Rsync.usable():
			self._logger.warn(
				"rsync exited with error code 24. This means a partial transfer due to vanished " + 
				"source files. In most cases files are copied normally"
			)
		if ret_code == 23 and Rsync.usable():
			self._logger.warn(
				"rsync seemed successful but exited with error code 23. This probably means " +
           		"that your version of rsync was built against a kernel with HAVE_LUTIMES defined, " +
             	"although the current kernel was not built with this option enabled. The bundling " +
			 	"process will thus ignore the error and continue bundling.  If bundling completes " +
           		"successfully, your image should be perfectly usable. We, however, recommend that " +
		   		"you install a version of rsync that handles this situation more elegantly.")
		elif ret_code == 1 and xattr:
			self._logger.warn(
				"rsync with preservation of extended file attributes failed. Retrying rsync " +
           		"without attempting to preserve extended file attributes...")
			self._rsync(src, dst, excludes, xattr=False)
		elif ret_code > 0:
			raise HandlerError('rsync failed with exit code %s' % (ret_code,))