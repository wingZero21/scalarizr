'''
Created on Apr 30, 2010

@author: marat
'''

from scalarizr.messaging import Message
import xml.dom.minidom as dom
import unittest

class TestMessage(unittest.TestCase):

	def test_toxml(self):
		msg = Message(
			"Log", dict(
				server_id = "32d18890-bf13-468e-9f91-981ae7851baa",
				platform = "ec2"
			), dict(
				entry = (
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
		self.assertEqual(len(body.childNodes), 3)
		entry0 = body.childNodes[0]
		self.assertEqual(entry0.nodeName, "entry")
		entry1 = body.childNodes[0]
		self.assertEqual(entry1.nodeName, "entry")

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


if __name__ == "__main__":
	unittest.main()