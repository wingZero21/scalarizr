
import unittest
import os
from ConfigParser import RawConfigParser

class TestHandlers(unittest.TestCase):
	def test_inject_config(self):
		handler_config = RawConfigParser()
		handler_config.read(os.path.dirname(__file__) + "/../../resources/handler.ini")
		
		from scalarizr.core import Bus, BusEntries
		#from scalarizr.util import inject_config
		#inject_config(Bus()[BusEntries.CONFIG], handler_config, "handler_")
	
	
if __name__ == "__main__":
	import scalarizr.core
	unittest.main()