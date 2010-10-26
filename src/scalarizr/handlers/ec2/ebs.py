'''
Created on Mar 1, 2010

@author: marat
'''

from scalarizr.bus import bus
from scalarizr import config, handlers
from scalarizr.messaging import Messages
from scalarizr.util import fstool


import os
import logging



def get_handlers ():
	return [EbsHandler()]

class EbsHandler(handlers.Handler):
	_logger = None
	_platform = None
	_queryenv = None
	_msg_service = None
	_config = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = bus.platform
		self._queryenv = bus.queryenv_service
		self._config = bus.config
		
		bus.on("init", self.on_init)
		bus.define_events(
			# Fires when EBS is attached to instance
			# @param device: device name, ex: /dev/sdf
			"block_device_attached", 
			
			# Fires when EBS is detached from instance
			# @param device: device name, ex: /dev/sdf 
			"block_device_detached",
			
			# Fires when EBS is mounted
			# @param device: device name, ex: /dev/sdf
			"block_device_mounted"
		)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name in (Messages.INT_BLOCK_DEVICE_UPDATED, Messages.MOUNTPOINTS_RECONFIGURE)

	def on_init(self):
		bus.on("before_host_init", self.on_before_host_init)
		try:
			handlers.script_executor.skip_events.add(Messages.INT_BLOCK_DEVICE_UPDATED)
		except AttributeError:
			pass
	
	def on_before_host_init(self, *args, **kwargs):
		self._logger.debug("Adding udev rule for EBS devices")
		try:
			cnf = bus.cnf
			scripts_path = cnf.rawini.get(config.SECT_GENERAL, config.OPT_SCRIPTS_PATH)
			if scripts_path[0] != "/":
				scripts_path = os.path.join(bus.base_path, scripts_path)
			f = open("/etc/udev/rules.d/84-ebs.rules", "w+")
			f.write('KERNEL=="sd*[!0-9]", ACTION=="add|remove", RUN+="'+ scripts_path + '/udev"')
			f.close()
		except (OSError, IOError), e:
			self._logger.error("Cannot add udev rule into '/etc/udev/rules.d' Error: %s", str(e))
			raise

	def on_MountPointsReconfigure(self, message):
		self._logger.info("Reconfiguring mountpoints")
		for ebs_mpoint in self._queryenv.list_ebs_mountpoints():
			self._logger.debug("Processing %s", ebs_mpoint)
			if ebs_mpoint.is_array:
				# TODO: implement EBS arrays
				self._logger.warning("EBS array %s skipped. EBS arrays not implemented yet", ebs_mpoint.name)
				continue
			try:
				ebs_volume = ebs_mpoint.volumes[0]
			except IndexError:
				self._logger.error("Invalid mpoint info %s. Volumes list is empty", ebs_mpoint)
				continue
			if not ebs_volume.volume_id or not ebs_volume.device:
				self._logger.error("Invalid volume info %s. volume_id and device should be non-empty", ebs_volume)
				continue
			devname = ebs_volume.device
			
			mtab = fstool.Mtab()			
			if not mtab.contains(devname, reload=True):
				self._logger.debug("Mounting device %s to %s", devname, ebs_mpoint.dir)
				try:
					fstool.mount(devname, ebs_mpoint.dir, make_fs=ebs_mpoint.create_fs, auto_mount=True)
				except fstool.FstoolError, e:
					if e.code == fstool.FstoolError.NO_FS:
						self._logger.debug('Creating filesystem and mount device %s to %s', devname, ebs_mpoint.dir)
						fstool.mount(devname, ebs_mpoint.dir, make_fs=True, auto_mount=True)
					else:
						raise
				self._logger.debug("Device %s is mounted to %s", devname, ebs_mpoint.dir)
				
				self.send_message(Messages.BLOCK_DEVICE_MOUNTED, dict(
					volume_id = ebs_volume.volume_id,
					device_name = devname
				), broadcast=True)
				bus.fire("block_device_mounted", volume_id=ebs_volume.volume_id, device=devname)				
				
			else:
				entry = mtab.find(devname)[0]
				self._logger.debug("Skip device %s already mounted to %s", devname, entry.mpoint)
				
		self._logger.debug("Mountpoints reconfigured")
		

				
	def on_IntBlockDeviceUpdated(self, message):
		if message.action == "add":
			self._logger.debug("udev notified me that block device %s was attached", message.devname)
			
			self.send_message(
				Messages.BLOCK_DEVICE_ATTACHED, 
				{"device_name" : message.devname}, 
				broadcast=True
			)
			
			bus.fire("block_device_attached", device=message.devname)
			
		elif message.action == "remove":
			self._logger.debug("udev notified me that block device %s was detached", message.device)
			fstab = fstool.Fstab()
			fstab.remove(message.devname)
			
			self.send_message(
				Messages.BLOCK_DEVICE_DETACHED, 
				{"device_name" : message.devname}, 
				broadcast=True
			)
			
			bus.fire("block_device_detached", device=message.devname)						

