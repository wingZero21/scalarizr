'''
Created on May 27, 2010

@author: marat
'''
from scalarizr.util import init_tests, configtool
import unittest
from scalarizr.messaging.p2p import P2pMessage
import uuid
from scalarizr.bus import bus
from scalarizr.messaging import MetaOptions


class Test(unittest.TestCase):

	def test_p2pmessage(self):
		new_server_id = str(uuid.uuid4())
		
		config = bus.config
		server_id_opt = configtool.option_wrapper(config, configtool.SECT_GENERAL, configtool.OPT_SERVER_ID)
		server_id_opt.set(new_server_id)
	
		m1 = P2pMessage("HostInit")
		m1.meta["server_id"] = str(uuid.uuid4())
		
		m2 = P2pMessage("HostUp")
		self.assertEqual(new_server_id, m2.meta[MetaOptions.SERVER_ID])


if __name__ == "__main__":
	init_tests()
	unittest.main()