from __future__ import with_statement
'''
Created on Oct 13, 2011

@author: marat
'''

from __future__ import with_statement

import os
import logging
import string

from scalarizr.bus import bus
from scalarizr import storage2, linux
from scalarizr import config
from scalarizr import handlers
from scalarizr.node import __node__
from scalarizr.messaging import Messages
from scalarizr.util import wait_until
from scalarizr.linux import mount, coreutils


LOG = logging.getLogger(__name__)

class BlockDeviceHandler(handlers.Handler):
	_platform = None
	_queryenv = None
	_msg_service = None
	_vol_type = None
	_config = None

	def __init__(self, vol_type):
		self._vol_type = vol_type
		self._volumes = []
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
		return message.name in (Messages.INT_BLOCK_DEVICE_UPDATED, 
				Messages.MOUNTPOINTS_RECONFIGURE, 
				Messages.BEFORE_HOST_TERMINATE)

	def on_init(self):
		bus.on(
			before_host_init=self.on_before_host_init,
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up
		)

		try:
			handlers.script_executor.skip_events.add(Messages.INT_BLOCK_DEVICE_UPDATED)
		except AttributeError:
			pass
		if __node__['state'] == 'running':
			volumes = self._queryenv.list_farm_role_params(__node__['farm_role_id']).get('params', {}).get('volumes', [])
			volumes = volumes or []  # Cast to list
			for vol in volumes:
				vol = storage2.volume(vol)
				vol.ensure(mount=bool(vol.mpoint))

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


	def on_host_init_response(self, hir):
		
		LOG.info('Configuring block device mountpoints')
		with bus.initialization_op as op:
			with op.phase(self._phase_plug_volume):
				wait_until(self._plug_all_volumes, sleep=10, timeout=600, 
						error_text='Cannot attach and mount disks in a reasonable time')
		
		volumes = hir.body.get('volumes') or []
		if volumes:
			LOG.debug('HIR volumes: %s', volumes)
			for i in range(0, len(volumes)):
				vol = volumes[i]
				template = vol.pop('template', None)
				from_template_if_missing = vol.pop('from_template_if_missing', None)
				vol = storage2.volume(**vol)
				LOG.info('Ensuring %s volume %s', vol.type, dict(vol))
				try:
					vol.ensure(mount=bool(vol.mpoint), mkfs=True)
				except storage2.VolumeNotExistsError, e:
					if template and from_template_if_missing == '1':
						vol = storage2.volume(**template)
						LOG.warn('Volume %s not exists, re-creating %s volume from config: %s', 
								str(e), vol.type, dict(vol))
						vol.ensure(mount=bool(vol.mpoint), mkfs=True)
					else:
						raise
				self._volumes.append(dict(vol))


	def on_before_host_up(self, hostup):
		if self._volumes:
			LOG.debug('update hostup with volumes. HostUp message body: %s', hostup.body)
			hostup.body['volumes'] = self._volumes


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
					self.send_message(Messages.BLOCK_DEVICE_MOUNTED, 
						{"device_name": vol.device, 
						"volume_id": vol.id, 
						"mountpoint": vol.mpoint}
					)
				
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

	def on_BeforeHostTerminate(self, message):
		if message.local_ip != __node__['private_ip']:
			return

		volumes = message.body.get('volumes', [])
		volumes = volumes or []
		
		for volume in volumes:
			volume = storage2.volume(volume)
			volume.umount()
			volume.detach()


