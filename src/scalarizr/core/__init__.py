class BusEntries:
	DB = "db"
	"""
	@cvar sqlalchemy.pool.SingletonThreadPool: Database connection pool
	"""
	
	MESSAGE_CONSUMER = "message_consumer"
	"""
	@cvar scalarizr.messaging.MessageConsumer: Default message consumer
	"""
	
	MESSAGE_PRODUCER = "message_producer"
	"""
	@cvar scalarizr.messaging.MessageProducer: Default message producer
	"""
	
	QUERYENV_SERVICE = "queryenv_service"
	"""
	@cvar: QueryEnv service client
	"""

class _Bus:
	_registry = {}
	
	def __setitem__(self, name, value):
		self._registry[name] = value
	
	def __getitem__(self, name):
		return self._registry[name]
	
	
# Bus singleton 	
_bus = None
def Bus ():
	global _bus
	if (_bus is None):
		_bus = _Bus()
	return _bus


import os.path
import sqlite3 as sqlite
import sqlalchemy.pool as pool
import logging
import logging.config

	
BASE_PATH =  os.path.realpath(os.path.dirname(__file__) + "/../../..")

def initialize ():
	global BASE_PATH
	
	# Configure logging
	logging.config.fileConfig(BASE_PATH + "/etc/logging.ini")
	# Configure database connection pool
	Bus()[BusEntries.DB] = pool.SingletonThreadPool(_connect)

def _connect():
	global BASE_PATH

	file = BASE_PATH + "/etc/.storage/db.sqlite3"

	logger = logging.getLogger(__package__)
	logger.debug("Open SQLite database (file: %s)" % (file))
	
	conn = sqlite.Connection(file)
	conn.row_factory = sqlite.Row
	return conn
	
initialize()
