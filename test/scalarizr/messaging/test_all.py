'''
Created on Dec 4, 2009

@author: marat
'''
import unittest
import timemodule
from threading import Thread
from scalarizr.messaging import Message
from scalarizr.messaging.p2p import P2pMessageService


class TestMessage(unittest.TestCase):

	def test_message_tostring(self):
		msg = Message("HostInit",
					{"serverType": "ec2", "os": "linux", "osVersion": "Ubuntu linux 8.10"},
					{"ec2.sshPub": "MIT...xx=="})
		msg.id = "12346xxxx-xxxx-xxx2221"
		#print msg

	def test_message_fromxml(self):
		xml = '<?xml version="1.0" ?>' \
				'<message id="12346xxxx-xxxx-xxx2221" name="HostInit">' \
				'<meta>' \
				'<item name="serverType">ec2</item>' \
				'<item name="os">linux</item>' \
				'<item name="osVersion">Ubuntu linux 8.10</item>' \
				'</meta>' \
				'<body>' \
				'<item name="ec2.sshPub">MIT...xx==</item>' \
				'</body>' \
				'</message>'
		
		msg = Message()
		msg.fromxml(xml)
		
		self.assertEqual(msg.id, "12346xxxx-xxxx-xxx2221")
		self.assertEqual(msg.name, "HostInit")
		self.assertEqual(msg.meta.keys(), ["serverType", "os", "osVersion"])
		self.assertEqual(msg.meta.values(), ["ec2", "linux", "Ubuntu linux 8.10"])
		self.assertEqual(msg.body.keys(), ["ec2.sshPub"])
		self.assertEqual(msg.body.values(), ["MIT...xx=="])

	def test_new_message(self):
		from scalarizr.messaging import MessageService
		ms = MessageService()
		msg = ms.new_message("HostInit")
		print msg



class TestInteration(unittest.TestCase):

	_consumer = None
	_producer = None
	_service = None
	_consumer_started = False

	def setUp(self):
		self._service = P2pMessageService((
				("p2p.server_id", "51310880-bb96-4a4e-8f1c-1b7ac094853b"),
				("p2p.crypto_key_path", "etc/.keys/default"),
				("p2p.consumer.endpoint", "http://localhost:8013"),
				("p2p.producer.endpoint", "http://localhost:8013")))
		self._consumer = self._service.get_consumer()
		from scalarizr.core.handlers import MessageListener
		self._consumer.add_message_listener(MessageListener())
		self._producer = self._service.get_producer()
		
		t = Thread(target=self._start_consumer)
		t.start()
		timemodule.sleep(1)

	def _start_consumer(self):
		self._consumer.start()
	
	def tearDown(self):
		self._consumer.stop()

	def testAll(self):
		message = self._service.new_message("HostUp", {"a" : "b"}, {"cx" : "dd"})
		self._producer.send("control", message)	


if __name__ == "__main__":
	import scalarizr.core
	unittest.main()