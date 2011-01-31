'''
@author: Dmytro Korsakov
'''
from scalarizr.util import LocalObject, SqliteLocalObject, init_tests
from sqlalchemy.pool import SingletonThreadPool
import unittest
import threading
import sqlite3 as sqlite
import logging
import time

class TestSQLite(unittest.TestCase):
	localobj = None

	def setUp(self):
		self.localobj = SingletonThreadPool(self._db_connect)
		##self.localobj = SqliteLocalObject(self._db_connect)
		#for loop test:
		conn = self.localobj.get().get_connection()
		cur = conn.cursor()
		sql = """create table test (key INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT);"""
		cur.execute(sql)
		conn.commit()
		#conn.close()
		
		
	def tearDown(self):
		#for loop test:
		conn = self.localobj.get().get_connection()
		cur = conn.cursor()
		sql = """drop table test;"""
		cur.execute(sql)
		conn.commit()
		conn.close()
		#normal:
		del self.localobj
	
	def test_get_from_the_same_thread(self):
		conn1 = self.localobj.get().get_connection()
		conn2 = self.localobj.get().get_connection()
		print conn1, conn2, "got from the same thread"
		self.assertEqual(conn1, conn2)	
	"""	
	def _test_get_from_the_same_thread2(self):
		conn1 = self.localobj.get()
		conn2 = self.localobj.get()
		self.assertEqual(conn1.get_connection(), conn2.get_connection())
	"""
	
	def _db_connect(self):
		logger = logging.getLogger(__name__)
		#logger.info("Open SQLite database in memory")
		#conn = sqlite.Connection(":memory:")
		logger.info("Open SQLite database in file")
		conn = sqlite.Connection("/home/shaitanich/workspace/test_db.db")
		conn.row_factory = sqlite.Row
		self.o_thread = conn
		return conn		

	def test_get_from_different_threads(self):
		o_main = self.localobj.get().get_connection()
		self.o_thread = None
	
		t = threading.Thread(target=self._db_connect)
		t.start()
		t.join()
		print o_main, self.o_thread, "got from different threads"
		self.assertNotEqual(o_main, self.o_thread)

	def _loop(self):
		conn2 = self.localobj.get().get_connection()

		for i in range(100):
			try:
				cur2 = conn2.cursor()
				sql2 = """INSERT INTO test (data) VALUES ('trololo');"""
				sqlsel2 = """select COUNT(*) from test where data = 'trololo';"""
				cur2.execute(sql2)
				cur2.execute(sqlsel2)
				print "Trololo count: ", cur2.fetchone()[0]
				conn2.commit()
				time.sleep(0.010)
			except BaseException, e:
				print "Trololo: ", str(e)
		#conn2.close()
	
	def test_loop(self):
		#for loop test:
		conn = self.localobj.get().get_connection()
		t = threading.Thread(target=self._loop)
		t.start()
		for i in range(100):
			cur = conn.cursor()
			sql = """INSERT INTO test (data) VALUES ('olololo');"""
			sqlsel = """select COUNT(*) from test where data = 'olololo';"""
			cur.execute(sql)
			cur.execute(sqlsel)
			print "Ololo count: ", cur.fetchone()[0]
			conn.commit()
			time.sleep(0.010)
		t.join()
		#conn.close()
		

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