'''
Created on Dec 4, 2009

@author: marat
'''
import unittest


class Test(unittest.TestCase):


	def setUp(self):
		pass


	def tearDown(self):
		pass


	def test_message_tostring(self):
		from scalarizr.messaging import Message
		msg = Message("HostInit", \
					{"serverType": "ec2", "os": "linux", "osVersion": "Ubuntu linux 8.10"}, \
					{"ec2.sshPub": "MIT...xx=="})
		msg.id = "12346xxxx-xxxx-xxx2221"
		print msg.__str__()
		pass

	def test_message_fromxml(self):
		from scalarizr.messaging import Message
		xml = '<?xml version="1.0" ?>' + \
				'<message id="12346xxxx-xxxx-xxx2221" name="HostInit">' + \
				'<meta>' + \
				'<item name="serverType">ec2</item>' + \
				'<item name="os">linux</item>' + \
				'<item name="osVersion">Ubuntu linux 8.10</item>' + \
				'</meta>' + \
				'<body>' + \
				'<item name="ec2.sshPub">MIT...xx==</item>' + \
				'</body>' + \
				'</message>'
		
		msg = Message()
		msg.fromxml(xml)
		
		self.assertEqual(msg.id, "12346xxxx-xxxx-xxx2221")
		self.assertEqual(msg.name, "HostInit")
		self.assertEqual(msg.meta.keys(), ["serverType", "os", "osVersion"])
		self.assertEqual(msg.meta.values(), ["ec2", "linux", "Ubuntu linux 8.10"])
		self.assertEqual(msg.body.keys(), ["ec2.sshPub"])
		self.assertEqual(msg.body.values(), ["MIT...xx=="])
		

if __name__ == "__main__":
	unittest.main()