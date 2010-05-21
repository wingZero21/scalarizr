'''
Created on Mar 2, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler
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
	
	def on_init(self, *args, **kwargs):
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_hello", self.on_before_hello)		

		msg_service = bus.messaging_service
		producer = msg_service.get_producer()
		producer.on("before_send", self.on_before_message_send)

	
	def on_host_init_response(self, message):
		"""
		@param message: HostInitResponse message 
		"""
		set_aws_credentials(message)
	
	
	def on_before_hello(self, message):
		"""
		@param message: Hello message
		"""
		
		message.aws_instance_id = self._platform.get_instance_id()
		message.aws_instance_type = self._platform.get_instance_type()		
		message.aws_ami_id = self._platform.get_ami_id()
		message.aws_avail_zone = self._platform.get_avail_zone()


	def on_before_message_send(self, queue, message):
		"""
		@todo: add aws specific here
		"""
		pass
	

def set_aws_credentials(message):
	platform = bus.platfrom
	logger = logging.getLogger(__name__)
	
	# Update ec2 platform configurations
	sect_name = configtool.get_platform_section_name(platform.name)
	private_filename = configtool.get_platform_filename(
				platform.name, ret=configtool.RET_PRIVATE)
		
	# Private	
	configtool.update(private_filename, {
		sect_name : {
			ec2_platform.OPT_ACCOUNT_ID : message.aws_account_id,
			ec2_platform.OPT_KEY_ID : message.aws_key_id,
			ec2_platform.OPT_KEY : message.aws_key
		}
	})
	
	#Public
	config = bus.config
	if message.aws_cert:
		configtool.write_key(config.get(sect_name, ec2_platform.OPT_CERT_PATH), 
				message.aws_cert, key_title="EC2 user certificate")
	else:
		logger.warn("EC2 user certificate is empty in 'HostInitResponse' message")
		
	if message.aws_pk:
		configtool.write_key(config.get(sect_name, ec2_platform.OPT_PK_PATH), 
				message.aws_pk, key_title="EC2 user private key")
	else:
		logger.warn("EC2 user private key is empty in 'HostInitResponse' message")	
	