'''
@author: Dmytro Korsakov
'''
from scalarizr.util import LocalObject, init_tests

import unittest
import threading
import sqlite3 as sqlite
import logging

"""

class TestLocalObject(unittest.TestCase):
	
	def setUp(self):
		self.localobj = LocalObject(Foo)
		
	def tearDown(self):
		del self.localobj
	
	def test_get_from_different_threads(self):
		o_main = self.localobj.get()
		o_thread = None
		
		def run():
			o_thread = self.localobj.get()
		
		t = threading.Thread(target=run)
		t.start()
		t.join()
		
		self.assertNotEqual(o_main, o_thread) 
	
	def test_get_from_the_same_threads(self):
		o1 = self.localobj.get()
		o2 = self.localobj.get()
		self.assertEqual(o1, o2)

class Foo:
	def __init__(self):
		self.prop = threading.currentThread()

"""

class TestSQLite(unittest.TestCase):
	localobj = None
	
	def setUp(self):
		self.localobj = LocalObject(_SQLiteConnection)
		
	def tearDown(self):
		del self.localobj		

	def test_get_from_the_same_thread(self):
		obj1 = self.localobj.get()
		obj2 = self.localobj.get()
		conn1 = obj1.get_connection()
		conn2 = obj2.get_connection()
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
			print "new conn"
			logger = logging.getLogger(__name__)
			logger.info("Open SQLite database in memory")
			conn = sqlite.Connection(":memory:")
			conn.row_factory = sqlite.Row		
			self._conn = conn
			
		return self._conn
		
		
	
if __name__ == "__main__":
	init_tests()
	unittest.main()