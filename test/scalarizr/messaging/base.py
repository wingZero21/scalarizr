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


	def test_message_to_string(self):
		from scalarizr.messaging import Message
		msg = Message("HostInit", \
					{"serverType": "ec2", "os": "linux", "osVersion": "Ubuntu linux 8.10"}, \
					{"ec2.sshPub": "MIT...xx=="})
		msg.id = "12346xxxx-xxxx-xxx2221"
		print msg.__str__()
		pass


if __name__ == "__main__":
	unittest.main()