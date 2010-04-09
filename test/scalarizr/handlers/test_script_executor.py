'''
Created on Dec 25, 2009

@author: marat
'''
import unittest
import os

class Test(unittest.TestCase):

	def test_all(self):
		from scalarizr.core import Bus, BusEntries
		bus = Bus()
		
		from ConfigParser import ConfigParser
		#from scalarizr.util import inject_config
		config = bus[BusEntries.CONFIG]
		cparser = ConfigParser()
		cparser.read(os.path.dirname(__file__) + "/../../../../etc/include/handler.script_executor.ini")
 		#inject_config(config, cparser)
		
		class _Bunch(dict):
			__getattr__, __setattr__ = dict.get, dict.__setitem__
			
		class _QueryEnv:
			def list_scripts(self, event_name):
				return [_Bunch(
					name="longplay.php",
					asynchronous=True,
					exec_timeout=2000,
					body=open(os.path.dirname(__file__) + "/../../../resources/handlers/longplay.php").read()
				), _Bunch(
					name="tomanyout.php",
					asynchronous=True,
					exec_timeout=60000,
					body=open(os.path.dirname(__file__) + "/../../../resources/handlers/tomanyout.php").read()
				)]
		
		bus[BusEntries.QUERYENV_SERVICE] = _QueryEnv()
		
		from scalarizr.messaging import Message
		class _MessageService:
			def new_message(self, name=None):
				return Message(name)
			
			def get_producer(self):
				class _Producer:
					def send(self, message):
						pass
				return _Producer()
		bus[BusEntries.MESSAGE_SERVICE] = _MessageService()
		
		
		from scalarizr.platform.vps import VpsPlatform
		bus[BusEntries.PLATFORM] = VpsPlatform()
		
		from scalarizr.core.handlers.script_executor import ScriptExecutor
		handler = ScriptExecutor(wait_async=True)
		
		from scalarizr.messaging import Message
		message = Message("EventNotice", {}, {
				"InternalIP": "10.23.75.199", 
				"RoleName": "app64", 
				"EventName": "MazzaFakkaLaunched"})
		
		handler(message)


if __name__ == "__main__":
	import scalarizr.core
	unittest.main()