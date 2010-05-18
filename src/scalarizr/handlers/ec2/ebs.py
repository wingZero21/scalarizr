'''
Created on Mar 1, 2010

@author: marat
'''

from scalarizr.handlers import Handler
from scalarizr.bus import bus
from scalarizr.messaging import Queues, Messages
from scalarizr.util import system, fstool, configtool
import logging
try:
	import time
except ImportError:
	import timemodule as time


def get_handlers ():
	return [EbsHandler()]

class EbsHandler(Handler):
	_logger = None
	_platform = None
	_queryenv = None
	_msg_service = None
	_config = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = bus.platfrom
		self._msg_service = bus.messaging_service
		self._queryenv = bus.queryenv_service
		self._config = bus.config
		
		bus.on("init", self.on_init)
		bus.define_events(
			# Fires when EBS is attached to instance
			# @param volume_id: EBS volume id
			# @param device: device name, ex: /dev/sdf
			"block_device_attached", 
			
			# Fires when EBS is detached from instance
			# @param device: device name, ex: /dev/sdf 
			"block_device_detached",
			
			# Fires when EBS is mounted
			# @param volume_id: EBS volume id
			# @param device: device name, ex: /dev/sdf
			"block_device_mounted"
		)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.INT_BLOCK_DEVICE_UPDATED and platform == "ec2"

	def on_init(self):
		bus.on("before_host_init", self.on_before_host_init)
	
	def on_before_host_init(self, *args, **kwargs):
		self._logger.info("Add udev rule for EBS devices")
		try:
			config = bus.config
			scripts_path = config.get(configtool.SECT_GENERAL, configtool.OPT_SCRIPTS_PATH)
			f = open("/etc/udev/rules.d/84-ebs.rules", "w+")
			f.write('KERNEL=="sd*[!0-9]", RUN+="'+ scripts_path + '/udev"')
			f.close()
		except (OSError, IOError), e:
			self._logger.error("Cannot add udev rule into '/etc/udev/rules.d' Error: %s", str(e))
			raise

	def on_IntBlockDeviceUpdated(self, message):
		ec2_conn = self._platform.get_ec2_conn()
		self._logger.debug(message)

		if message.action == "add":
			# Volume was attached
			max_attempts = 5
			attempt = 1
			while attempt <= max_attempts:
				for volume in ec2_conn.get_all_volumes():
					ad = volume.attach_data
					if not ad is None and \
							ad.instance_id == self._platform.get_instance_id() and \
							ad.device == message.devname and \
							ad.status == "attached":
						
						self._logger.info("EBS was attached (volumeId: %s, device: %s)" % (volume.id, ad.device))
						# Send message to Scalr
						msg = self._msg_service.new_message(Messages.BLOCK_DEVICE_ATTACHED, body=dict(
							volume_id = volume.id,
							device_name = ad.device
						))
						self._put_broadcast_data(msg)
						producer = self._msg_service.get_producer()
						producer.send(Queues.CONTROL, msg)
						
						# Notify listeners
						bus.fire("block_device_attached", volume=volume.id, device=ad.device)
						
						try:
							# Check EBS mountpoints and mount device if necessary
							if self._mount_volume(volume):
								msg = self._msg_service.new_message(Messages.BLOCK_DEVICE_MOUNTED, body=dict(
									volume_id = volume.id,
									device_name = ad.device
								))
								self._put_broadcast_data(msg)
								producer.send(Queues.CONTROL, msg)
								
								bus.fire("block_device_mounted", volume_id=volume.id, device=ad.device)
						except BaseException, Exception:
							self._logger.error("Mount EBS volume failed")
							raise
						
						return
				
				if attempt < max_attempts:
					self._logger.debug("Attempt %d not succeed. " +
								"Sleep %d seconds before the next one", attempt, attempt)		
					time.sleep(attempt)
				else:
					self._logger.debug("Attempt %d not succeed", attempt)
				attempt = attempt + 1
			else:
				self._logger.warning("Unable to verify that EBS was attached to the server")
				
		elif message.action == "remove":
			# Volume was detached
			self._logger.info("EBS was detached (device: %s)", message.device)
			
			# Send message to Scalr
			msg = self._msg_service.new_message(Messages.BLOCK_DEVICE_DETACHED)
			msg.device_name = message.devname
			producer = self._msg_service.get_producer()
			producer.send(Queues.CONTROL, msg)
			
			# Notify listeners
			bus.fire("block_device_detached", device=message.devname)


	def _mount_volume(self, ec2_volume):
		gotcha = False
		for mpoint in self._queryenv.list_ebs_mountpoints():
			for volume in mpoint.volumes:
				if volume.volume_id == ec2_volume.id:
					gotcha = True
		
		if gotcha:
			self._logger.info("Mounting EBS volume '%s' as device '%s' on '%s' ...", 
					ec2_volume.id, volume.device, mpoint.dir)
		else:
			self._logger.warn("Cannot find volume '%s' in EBS mountpoints list", ec2_volume.id)
			return False 
					
		if mpoint.create_fs:
			self._logger.info("Creating new filesystem on device '%s'", volume.device)
			system("/sbin/mkfs.ext3 -F " + volume.device + " 2>&1")
			
		fstool.mount(volume.device, mpoint.dir, ["-t auto"])
		fstab = fstool.Fstab()
		if not any([entry.device == volume.device for entry in fstab.list_entries()]):
			self._logger.info("Adding a record to fstab")
			fstab.append(fstool.TabEntry(volume.device, mpoint.dir, "auto", "defaults\t0\t0"))

		self._logger.info("Device %s succesfully mounted to %s (volume_id: %s)", 
				volume.device, mpoint.dir, ec2_volume.id)
		return True
