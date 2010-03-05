'''
Created on Mar 2, 2010

@author: marat
'''

from scalarizr.core.handlers import Handler
from scalarizr.core import Bus, BusEntries
from scalarizr.platform.ec2 import Aws
import logging
from scalarizr.messaging import Messages

def get_handlers ():
	return [AwsLifeCircleHandler()]

class AwsLifeCircleHandler(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		Bus().on("init", self.on_init)		
	
	def on_init(self, *args, **kwargs):
		bus = Bus()
		bus.on("before_host_init", self.on_before_host_init)		

		msg_service = bus[BusEntries.MESSAGE_SERVICE]
		producer = msg_service.get_producer()
		producer.on("before_send", self.on_before_message_send)
	
		
	def on_before_host_init(self, *args, **kwargs):
		bus = Bus()
		base_path = bus[BusEntries.BASE_PATH]
		self._logger.info("Add udev rule for EBS devices")
		try:
			f = open("/etc/udev/rules.d/84-ebs.rules", "w+")
			f.write('KERNEL=="sd*[!0-9]", RUN+="'+base_path+'/src/scalarizr/scripts/udev.py"')
			f.close()
		except IOError, e:
			self._logger.error("Cannot add udev rule into '/etc/udev/rules.d' IOError: %s", str(e))
			raise

	
	def on_before_message_send(self, queue, message):
		
		"""
		@todo: add aws specific here
		"""
		pass
	
	
	def on_HostInitResponse(self, message):
		aws = Aws()
		aws.set_keys(message.body["aws.key_id"], message.body["aws.key"])
		
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.HOST_INIT_RESPONSE and platform == "ec2"