


class _Bus:
	_registry = {}
	
	def set (self, name, value):
		self._registry[name] = value
	
	def get (self, name):
		return self._registry[name]
	
# Bus singleton 	
_bus_instance = None
def Bus ():
	if (_bus_instance is None):
		_bus_instance = _Bus()
	return _bus_instance

class BusEntries:
	DB = "db"
	MESSAGE_CONSUMER = "message_consumer"
	MESSAGE_PRODUCER = "message_producer"
	QUERYENV_SERVICE = "queryenv_service"