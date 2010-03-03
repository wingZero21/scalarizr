'''
Created on Mar 3, 2010

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.core.handlers import Handler

def get_handlers():
	return [LifeCircleHandler()]

class LifeCircleHandler(Handler):
	_bus = None
	
	def __init__(self):
		bus = Bus()
		bus.define_events(
			# Fires before HostInit message is sent
			"beforehostinit",
			
			# Fires after HostInit message is sent
			"hostinit"
		)
		bus.on("start", self.on_start)
		self._bus = bus
		
		msg_service = bus[BusEntries.MESSAGE_SERVICE]
		producer = msg_service.get_producer()
		producer.on("beforesend", self.on_before_message_send)
	
	def on_before_message_send(self, queue, message):
		"""
		@todo: Add scalarizr version to meta
		"""
		pass
		
	
		
	def on_start(self):
		self._bus.fire("beforehostinit", "a", "b")
		
		# Send host init
		"""
		producer = service.get_producer()
		msg = service.new_message(Messages.HOST_INIT)
		# Regenerage key
		from scalarizr.util import CryptoUtil
		key = CryptoUtil().keygen()
		open(base_path + "/" + config.get("default", "crypto_key_path"), "w+").write(key)
		msg.key = key
		producer.send(Queues.CONTROL, msg) 
		"""		
		
		self._bus.fire("hostinit")

	