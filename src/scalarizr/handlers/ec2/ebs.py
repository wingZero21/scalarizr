'''
Created on Mar 1, 2010

@author: marat
'''

from scalarizr.bus import bus
from scalarizr import config, handlers
from scalarizr.messaging import Messages
from scalarizr.util import fstool, wait_until


import os
import logging
from scalarizr.storage import Storage, Volume
from scalarizr.platform.ec2 import ebstool
from threading import Thread
import threading
from scalarizr.handlers import HandlerError



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
		self.on_reload()
		
		bus.on(init=self.on_init, reload=self.on_reload)
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
		
	def on_reload(self):
		self._platform = bus.platform
		self._queryenv = bus.queryenv_service

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name in (Messages.INT_BLOCK_DEVICE_UPDATED, Messages.MOUNTPOINTS_RECONFIGURE)

	def on_init(self):
		bus.on("before_host_init", self.on_before_host_init)
		bus.on("host_init_response", self.on_host_init_response)
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

	def on_host_init_response(self, *args, **kwargs):
		self._logger.info('Configuring EBS mountpoints')		
		wait_until(self._plug_all_ebs, sleep=10, timeout=600, 
				error_text='Cannot attach and mount EBS disks in a reasonable time')
	
	def _plug_all_ebs(self):
		unplugged = 0
		plugged_names = []
		for ebs_mpoint in self._queryenv.list_ebs_mountpoints():
			if ebs_mpoint.name in plugged_names:
				continue
			if ebs_mpoint.name == 'vol-creating':
				unplugged += 1
			else:
				self._plug_ebs(ebs_mpoint)
				plugged_names.append(ebs_mpoint.name)
		return not unplugged
	
	def _plug_ebs(self, ebs_mpoint):
		try:
			if ebs_mpoint.is_array:
				return self._logger.warning('EBS array %s skipped. EBS arrays not implemented in Scalarizr', 
										ebs_mpoint.name)

			assert len(ebs_mpoint.volumes), 'Invalid mpoint info %s. Volumes list is empty' % ebs_mpoint
			ebs_volume = ebs_mpoint.volumes[0]
			assert ebs_volume.volume_id, 'Invalid volume info %s. volume_id should be non-empty' % ebs_volume
			
			vol = Storage.create(
				type='ebs', 
				id=ebs_volume.volume_id, 
				device=ebs_volume.device, 
				mpoint=ebs_mpoint.dir
			)
				
			mtab = fstool.Mtab()	
			if not mtab.contains(vol.device, reload=True):
				self._logger.debug("Mounting device %s to %s", vol.device, vol.mpoint)
				try:
					fstool.mount(vol.device, vol.mpoint, auto_mount=True)
				except fstool.FstoolError, e:
					if e.code == fstool.FstoolError.NO_FS:
						vol.mkfs()
						fstool.mount(vol.device, vol.mpoint, make_fs=True, auto_mount=True)
					else:
						raise
				self._logger.info("Device %s is mounted to %s", vol.device, vol.mpoint)
				
				self.send_message(Messages.BLOCK_DEVICE_MOUNTED, dict(
					volume_id = vol.id,
					device_name = vol.ebs_device
				), broadcast=True)
				bus.fire("block_device_mounted", volume_id=ebs_volume.volume_id, device=vol.device)				
				
			else:
				entry = mtab.find(vol.device)[0]
				self._logger.debug("Skip device %s already mounted to %s", vol.device, entry.mpoint)
		except:
			self._logger.exception("Can't attach EBS")
		
	def on_MountPointsReconfigure(self, message):
		self._logger.info("Reconfiguring EBS mountpoints")
		for ebs_mpoint in self._queryenv.list_ebs_mountpoints():
			self._plug_ebs(ebs_mpoint)
		self._logger.debug("Mountpoints reconfigured")
		
	def on_IntBlockDeviceUpdated(self, message):
		if not message.devname:
			return
		
		if message.action == "add":
			self._logger.debug("udev notified me that block device %s was attached", message.devname)
			
			self.send_message(
				Messages.BLOCK_DEVICE_ATTACHED, 
				{"device_name" : ebstool.get_ebs_devname(message.devname)}, 
				broadcast=True
			)
			
			bus.fire("block_device_attached", device=message.devname)
			Storage.fire('attach', Volume(device=message.devname))
			
		elif message.action == "remove":
			self._logger.debug("udev notified me that block device %s was detached", message.device)
			fstab = fstool.Fstab()
			fstab.remove(message.devname)
			
			self.send_message(
				Messages.BLOCK_DEVICE_DETACHED, 
				{"device_name" : ebstool.get_ebs_devname(message.devname)}, 
				broadcast=True
			)
			
			bus.fire("block_device_detached", device=message.devname)
			Storage.fire('detach', Volume(device=message.devname))

