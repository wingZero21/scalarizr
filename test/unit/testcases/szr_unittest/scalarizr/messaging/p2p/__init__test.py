'''
Created on Oct 14, 2010

@author: marat
'''
import unittest, os

from scalarizr.bus import bus
from szr_unittest import db_connect, RESOURCE_PATH
from scalarizr.messaging.p2p import P2pMessageStore

class P2pMessageStoreTest(unittest.TestCase):

	def setUp(self):
		file = os.path.join(RESOURCE_PATH, 'messaging/p2p/db-for-rotate.sqlite')
		bus.db = db_connect(file)
	
	def tearDown(self):
		pass

	def test_rotate(self):
		store = P2pMessageStore()
		conn = bus.db.get().get_connection()
		cur = conn.cursor()
		cur.execute('SELECT COUNT(*) FROM p2p_message')
		self.assertEqual(cur.fetchone()[0], 114)
		store.rotate()
		cur.execute('SELECT COUNT(*) FROM p2p_message')
		self.assertEqual(cur.fetchone()[0], 50)

if __name__ == "__main__":
	import szr_unittest
	unittest.main()