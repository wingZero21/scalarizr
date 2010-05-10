'''
Created on Mar 2, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.util import configtool
import scalarizr.platform.ec2 as ec2_platform
import logging

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
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.HOST_INIT_RESPONSE and platform == "ec2"	
	
	def on_init(self, *args, **kwargs):
		bus.on("before_host_init", self.on_before_host_init)		

		msg_service = bus.messaging_service
		producer = msg_service.get_producer()
		producer.on("before_send", self.on_before_message_send)
	
		
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

	
	def on_before_message_send(self, queue, message):
		
		"""
		@todo: add aws specific here
		"""
		pass
	
	
	def on_HostInitResponse(self, message):
		"""
		TODO: Send this to Igor
		message properties:
			ec2_account_id, ec2_key_id, ec2_key, ec2_cert, ec2_pk 
		"""
		
		# Update ec2 platform configurations
		sect_name = configtool.get_platform_section_name(self._platform.name)
		private_filename = configtool.get_platform_filename(
					self._platform.name, ret=configtool.RET_PRIVATE)
			
		# Private	
		configtool.update(private_filename, {
			sect_name : {
				ec2_platform.OPT_ACCOUNT_ID : message.ec2_account_id,
				ec2_platform.OPT_KEY_ID : message.ec2_key_id,
				ec2_platform.OPT_KEY : message.ec2_key
			}
		})
		
		#Public
		config = bus.config
		configtool.write_key(config.get(sect_name, ec2_platform.OPT_CERT_PATH), 
				message.ec2_cert, key_title="EC2 user certificate")
		configtool.write_key(config.get(sect_name, ec2_platform.OPT_PK_PATH), 
				message.ec2_pk, key_title="EC2 user private key")
		
