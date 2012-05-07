'''
Created on Dec 25, 2009

@author: marat
'''
import unittest
from scalarizr.bus import bus
from scalarizr.util import init_tests
from scalarizr.messaging import Message
from scalarizr.platform.vps import VpsPlatform
from scalarizr.handlers.script_executor import ScriptExecutor
import os

class Test(unittest.TestCase):

	def test_all(self):
		config = bus.config
		path = os.path.realpath(os.path.dirname(__file__) + "/../../resources/etc/public.d/handler.script_executor.ini") 
		print path
		config.read(path)
		
		bus.messaging_service = _MessageService()
		bus.platform = VpsPlatform()
		handler = ScriptExecutor(wait_async=True)

		message = Message("EventNotice", {}, {
				"InternalIP": "10.23.75.199", 
				"RoleName": "app64", 
				"EventName": "MazzaFakkaLaunched",
				"local_ip": "10.23.75.199",
				"scripts":"#!/bin/sh\n\necho 'text: ñ ç \u304b\u3099'"})
		
		handler(message)


class _Bunch(dict):
	__getattr__, __setattr__ = dict.get, dict.__setitem__
	
class _QueryEnv:
	def list_scripts(self, event_name, event_id):
		return [_Bunch(
			name="longplay.php",
			asynchronous=True,
			exec_timeout=2000,
			body=open(os.path.realpath(os.path.dirname(__file__) + "/../../resources/handlers/longplay.php")).read()
		), _Bunch(
			name="tomanyout.php",
			asynchronous=True,
			exec_timeout=60000,
			body=open(os.path.realpath(os.path.dirname(__file__) + "/../../resources/handlers/tomanyout.php")).read()
		)]

bus.queryenv_service = _QueryEnv()

class _MessageService:
	def new_message(self, msg_name, msg_meta, msg_body):
		return Message(msg_name)
	
	def get_producer(self):
		class _Producer:
			def send(self,queue, msg):
				pass
		return _Producer()

if __name__ == "__main__":
	init_tests()
	unittest.main()