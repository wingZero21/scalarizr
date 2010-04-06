'''
Created on Mar 2, 2010

@author: marat
'''

from scalarizr.handlers import Handler
from scalarizr.bus import bus
import logging
from scalarizr.messaging import Messages

def get_handlers ():
	return [AwsLifeCircleHandler()]

class AwsLifeCircleHandler(Handler):
	_logger = None
	_platform = None
	"""
	@ivar scalarizr.platform.ec2.AwsPlatform:
	"""
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = bus.platfrom
		bus.on("init", self.on_init)		
	
	def on_init(self, *args, **kwargs):
		bus.on("before_host_init", self.on_before_host_init)		

		msg_service = bus.messaging_service
		producer = msg_service.get_producer()
		producer.on("before_send", self.on_before_message_send)
	
		
	def on_before_host_init(self, *args, **kwargs):
		self._logger.info("Add udev rule for EBS devices")
		try:
			f = open("/etc/udev/rules.d/84-ebs.rules", "w+")
			f.write('KERNEL=="sd*[!0-9]", RUN+="'+ bus.base_path+'/src/scalarizr/scripts/udev.py"')
			f.close()
		except OSError, e:
			self._logger.error("Cannot add udev rule into '/etc/udev/rules.d' OSError: %s", str(e))
			raise

	
	def on_before_message_send(self, queue, message):
		
		"""
		@todo: add aws specific here
		"""
		pass
	
	
	def on_HostInitResponse(self, message):
		"""
		@todo: take all data from message
		"""
		self._platform.set_config(dict(
			account_id="",
			key_id="",
			key="",
			cert="",
			pk=""
		))
		
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.HOST_INIT_RESPONSE and platform == "ec2"