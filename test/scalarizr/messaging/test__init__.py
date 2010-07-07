'''
Created on Apr 30, 2010

@author: marat
'''

from scalarizr.messaging import Message
import xml.dom.minidom as dom
import unittest
from scalarizr.util import cryptotool, init_tests
import logging

class TestMessage(unittest.TestCase):

	def test_toxml(self):
		msg = Message(
			"Log", dict(
				server_id = "32d18890-bf13-468e-9f91-981ae7851baa",
				platform = "ec2"
			), dict(
				entries = (
					dict(category="scalarizr", level="INFO", message="Starting scalairzr..."),
					dict(category="scalarizr", level="INFO", message="Initialize services"),
					dict(category="scalarizr.messaging", level="ERROR", 
						message="Cannot initiate messaging server. Address port 8755 is already in use")
				)
			)
		)
		msg.id = "12346xxxx-xxxx-xxx2221"
		
		xml_string = msg.toxml()
		
		xml = dom.parseString(xml_string)

		root = xml.documentElement
		self.assertEqual(root.getAttribute("id"), msg.id)
		self.assertEqual(root.getAttribute("name"), msg.name)
		self.assertTrue(len(root.childNodes) == 2)
		
		meta = xml.documentElement.firstChild
		server_id = [el for el in meta.childNodes if el.nodeName == "server_id"]
		self.assertTrue(len(server_id), 1)
		self.assertEqual(server_id[0].firstChild.nodeValue, msg.meta["server_id"])
		
		body = xml.documentElement.childNodes[1]
		entries = body.firstChild
		self.assertTrue(any(el.nodeName == "item" for el in entries.childNodes))


	def test_fromxml(self):
		xml_string =  """<?xml version="1.0" ?>
		<message id="12346xxxx-xxxx-xxx2221" name="Log">
			<meta>
				<platform>ec2</platform>
				<server_id>32d18890-bf13-468e-9f91-981ae7851baa</server_id>
			</meta>
			<body>
				<ec2_account_id>5435544</ec2_account_id>
			</body>
		</message>
		"""
		
		msg = Message()
		msg.fromxml(xml_string)
		
		self.assertEqual(msg.name, "Log")
		self.assertEqual(msg.id, "12346xxxx-xxxx-xxx2221")
		self.assertEqual(msg.meta["server_id"], "32d18890-bf13-468e-9f91-981ae7851baa")
		self.assertEqual(msg.meta["platform"], "ec2")
		self.assertEqual(msg.body["ec2_account_id"], "5435544")

	def test_fromxml_with_empty_values (self):
		xml_string = """<?xml version="1.0" ?><message id="710e8b54-331c-4c23-afcd-adc98163c063" name="IntBlockDeviceUpdated"><meta><server_id>32d18890-bf13-468e-9f91-981ae7851baa</server_id></meta><body><subsystem>block</subsystem><devlinks>/dev/disk/by-uuid/f31203e0-b554-465a-856e-2d8d144714d7</devlinks><major>8</major><physdevpath>/devices/xen/vbd-2160</physdevpath><id_fs_version>1.0</id_fs_version><devpath>/block/sdh</devpath><udev_log>3</udev_log><devname>/dev/sdh</devname><physdevdriver>vbd</physdevdriver><udevd_event>1</udevd_event><action>remove</action><id_fs_label></id_fs_label><id_fs_usage>filesystem</id_fs_usage><id_fs_uuid>f31203e0-b554-465a-856e-2d8d144714d7</id_fs_uuid><physdevbus>xen</physdevbus><id_fs_label_safe></id_fs_label_safe><id_fs_type>ext3</id_fs_type><seqnum>1296</seqnum><minor>112</minor></body></message>"""
		
		msg = Message()
		msg.fromxml(xml_string)
		self.assertEqual(msg.name, "IntBlockDeviceUpdated")
		self.assertFalse(msg.meta.has_key("platform"))
		self.assertEqual(msg.subsystem, "block")

	def test_decode_wrong_spaces(self):
		key = "q9mBWijQrEphNSN77OiEHvA0r0U3PJb3ydvH2kkQz5wxqxpKfSFLGQ=="
		
		msg = Message("Rebundle", dict(), dict(role_name="scalarizr"))
		serialized = msg.toxml()
		crypted = cryptotool.encrypt(serialized, key)
		
		decrypted = cryptotool.decrypt(crypted, key)
		msg = Message()
		msg.fromxml(decrypted)
		logger = logging.getLogger(__name__)
		cmd = "mount %s" % (msg.role_name)
		logger.debug(cmd)

if __name__ == "__main__":
	init_tests()
	unittest.main()