from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.config import ScalarizrState

import logging

def get_handlers():
	return [MongoDbHandler()]

class MongoDbHandler(Handler):
	_logger = None
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on(init=self.on_init)
	
	def on_init(self):
		bus.on(
			start=self.on_start,
			mysql_configure=self.on_mysql_configure,
			before_mysql_data_bundle=self.on_before_mysql_data_bundle
		)
	
	def on_start(self, *args):
		cnf = bus.cnf
		if cnf.state == ScalarizrState.RUNNING:
			self._logger.info('Starting MongoDB')

	def on_mysql_configure(self, *args):
		self._logger.info('Start MongoDB')
	
	def on_before_mysql_data_bundle(self, *args):
		self._logger.info('Prepare MongoDB for snapshot')
		