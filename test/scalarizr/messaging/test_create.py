'''
Created on Mar 3, 2010

@author: marat
'''

import unittest
from scalarizr.messaging.p2p import P2pMessageService

class TestMessage(unittest.TestCase):
	def test_create_message(self):
		s= P2pMessageService((
			("p2p.server_id", "51310880-bb96-4a4e-8f1c-1b7ac094853b"),
			("p2p.crypto_key_path", "etc/.keys/default"),
			("p2p.consumer.endpoint", "http://localhost:8013"),
			("p2p.producer.endpoint", "http://localhost:8013"))
		)
		msg = s.new_message("BlockDeviceAttached", body=dict(
			device="/dev/sdh"
		))
		self.assertEqual(msg.name, "BlockDeviceAttached")
		self.assertEqual(msg.device, "/dev/sdh")


if __name__ == "__main__":
	import scalarizr.core
	unittest.main()