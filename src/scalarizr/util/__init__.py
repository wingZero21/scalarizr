
from threading import RLock

class Observable:
	
	_listeners = {}
	_lock = None
	
	def __ini__(self):
		self._lock = RLock()
	
	def define_events(self, *args):
		try: 
			self._lock.acquire()
			#for event in args:
		finally:
			self._lock.release()
	
	def fire(self, event, *args):
		pass
	
	def on(self, event, listener):
		pass
	
	def un(self):
		pass
	
	def suspend_events(self):
		pass
	
	def resume_events(self):
		pass

def config_apply(obj, config):
	for k in config.keys():
		if hasattr(obj, k):
			setattr(obj, k, config[k])
			
def config_applyif(obj, config):
	for k in config.keys():
		if hasattr(obj, k) and getattr(obj, k) in None:
			setattr(obj, k, config[k])
	