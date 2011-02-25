from scalarizr.bus import bus
from scalarizr.handlers import Handler

def get_handlers ():
	return [NimbulaLifeCycleHandler()]

class NimbulaLifeCycleHandler(Handler):
	_platform = None
	
	def __init__(self):
		self._platform = bus.platform
		bus.on("init", self.on_init)			
	
	def on_init(self, *args, **kwargs):
		bus.on("before_hello", self.on_before_hello)
		
	def on_before_hello(self, message):
		"""
		@param message: Hello message
		"""
		message.instance_id = self._platform.get_instance_id()
