
class _Bus:
	_registry = {}
	
	def __setattr__(self, name, value):
		self._registry[name] = value
	
	def __getattr__(self, name):
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



def initialize ():
	from sqlite3 import Connection, DatabaseError
	Bus()[BusEntries.DB] = Connection("etc/.storage/db.sqlite3")
	
	
	

