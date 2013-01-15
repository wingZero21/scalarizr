from __future__ import with_statement
'''
Created on Oct 13, 2011

@author: marat
'''

from __future__ import with_statement

import os
import logging

from scalarizr.bus import bus
from scalarizr import storage2
from scalarizr import config
from scalarizr import handlers
from scalarizr.messaging import Messages
from scalarizr.util import wait_until
from scalarizr.util import mount


LOG = logging.getLogger(__name__)

class BlockDeviceHandler(handlers.Handler):
	_platform = None
	_queryenv = None
	_msg_service = None
	_vol_type = None
	_config = None

	def __init__(self, vol_type):
		self._vol_type = vol_type
		self.on_reload()
		
		bus.on(init=self.on_init, reload=self.on_reload)
		bus.define_events(
			# Fires when volume is attached to instance
			# @param device: device name, ex: /dev/sdf
			"block_device_attached", 
			
			# Fires when volume is detached from instance
			# @param device: device name, ex: /dev/sdf 
			"block_device_detached",
			
			# Fires when volume is mounted
			# @param device: device name, ex: /dev/sdf
			"block_device_mounted"
		)
		
		self._phase_plug_volume = 'Configure storage'
		
		
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
		LOG.debug("Adding udev rule for EBS devices")
		try:
			cnf = bus.cnf
			scripts_path = cnf.rawini.get(config.SECT_GENERAL, config.OPT_SCRIPTS_PATH)
			if scripts_path[0] != "/":
				scripts_path = os.path.join(bus.base_path, scripts_path)
			f = open("/etc/udev/rules.d/84-ebs.rules", "w+")
			f.write('KERNEL=="sd*", ACTION=="add|remove", RUN+="'+ scripts_path + '/udev"\n')
			f.write('KERNEL=="xvd*", ACTION=="add|remove", RUN+="'+ scripts_path + '/udev"')
			f.close()
		except (OSError, IOError), e:
			LOG.error("Cannot add udev rule into '/etc/udev/rules.d' Error: %s", str(e))
			raise


	def on_host_init_response(self, *args, **kwargs):
		LOG.info('Configuring block device mountpoints')
		with bus.initialization_op as op:
			with op.phase(self._phase_plug_volume):
				wait_until(self._plug_all_volumes, sleep=10, timeout=600, 
						error_text='Cannot attach and mount disks in a reasonable time')


	def _plug_all_volumes(self):
		unplugged = 0
		plugged_names = []
		for qe_mpoint in self._queryenv.list_ebs_mountpoints():
			if qe_mpoint.name in plugged_names:
				continue
			if qe_mpoint.name == 'vol-creating':
				unplugged += 1
			else:
				self._plug_volume(qe_mpoint)
				plugged_names.append(qe_mpoint.name)
		return not unplugged


	def _plug_volume(self, qe_mpoint):
		try:
			assert len(qe_mpoint.volumes), 'Invalid mpoint info %s. Volumes list is empty' % qe_mpoint
			qe_volume = qe_mpoint.volumes[0]
			mpoint = qe_mpoint.dir or None
			assert qe_volume.volume_id, 'Invalid volume info %s. volume_id should be non-empty' % qe_volume
			
			vol = storage2.volume(
				type=self._vol_type, 
				id=qe_volume.volume_id, 
				name=qe_volume.device,
				mpoint=mpoint
			)

			if mpoint:
				def block():
					vol.ensure(mount=True, mkfs=True, fstab=True)
					bus.fire("block_device_mounted", 
							volume_id=vol.id, device=vol.device)
				
				if bus.initialization_op:
					msg = 'Mount device %s to %s' % (vol.device, vol.mpoint)
					with bus.initialization_op.step(msg):
						block()
				else:
					block()
				
		except:
			LOG.exception("Can't attach volume")


	def get_devname(self, devname):
		return devname


	def on_MountPointsReconfigure(self, message):
		LOG.info("Reconfiguring mountpoints")
		for qe_mpoint in self._queryenv.list_ebs_mountpoints():
			self._plug_volume(qe_mpoint)
		LOG.debug("Mountpoints reconfigured")


	def on_IntBlockDeviceUpdated(self, message):
		if not message.devname:
			return
		
		if message.action == "add":
			LOG.debug("udev notified me that block device %s was attached", message.devname)
			
			self.send_message(
				Messages.BLOCK_DEVICE_ATTACHED, 
				{"device_name" : self.get_devname(message.devname)}, 
				broadcast=True
			)
			
			bus.fire("block_device_attached", device=message.devname)
			
		elif message.action == "remove":
			LOG.debug("udev notified me that block device %s was detached", message.devname)
			fstab = mount.fstab()
			fstab.remove(message.devname)
			
			self.send_message(
				Messages.BLOCK_DEVICE_DETACHED, 
				{"device_name" : self.get_devname(message.devname)}, 
				broadcast=True
			)
			
			bus.fire("block_device_detached", device=message.devname)
