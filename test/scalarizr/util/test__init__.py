'''
@author: Dmytro Korsakov
'''
from scalarizr.util import LocalObject, SQLiteLO, init_tests

import unittest
import threading
import sqlite3 as sqlite
import logging

class TestSQLite(unittest.TestCase):
	localobj = None
	
	def setUp(self):
		self.localobj = SQLiteLO(self._db_connect)
		
	def tearDown(self):
		del self.localobj		

	def test_get_from_the_same_thread(self):
		conn1 = self.localobj.get().get_connection()
		conn2 = self.localobj.get().get_connection()
		self.assertEqual(conn1, conn2)

	def _db_connect(self):
		logger = logging.getLogger(__name__)
		logger.info("Open SQLite database in memory")
		conn = sqlite.Connection(":memory:")
		conn.row_factory = sqlite.Row
		self.o_thread = conn

	def test_get_from_different_threads(self):
		o_main = self.localobj.get()
		self.o_thread = None
	
		t = threading.Thread(target=self._db_connect)
		t.start()
		t.join()
		
		self.assertNotEqual(o_main, self.o_thread) 
		
class _SQLiteConnection(object):
	_conn = None

	def get_connection(self):
		if not self._conn:
			logger = logging.getLogger(__name__)
			logger.info("Open SQLite database in memory")
			conn = sqlite.Connection(":memory:")
			conn.row_factory = sqlite.Row		
			self._conn = conn
			
		return self._conn
		
		
	
if __name__ == "__main__":
	init_tests()
	unittest.main()