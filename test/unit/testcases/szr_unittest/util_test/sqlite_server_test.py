'''
Created on Mar 21, 2012

@author: dmitry
'''
import sys
import time
import threading
import sqlite3
import unittest
from scalarizr.util import sqlite_server, wait_until


conn = None

def get_connection():
	return sqlite3.Connection(database='/Users/dmitry/Documents/workspace/scalarizr-localobj/share/db.sql')

	
def test_fetchall(conn):
	cursor = conn.cursor()
	cursor.execute('select 1;')
	return cursor.fetchall()


def test_fetchone(conn):	
	cursor = conn.cursor()
	cursor.execute('select 1;')
	return cursor.fetchone()

		
class ThreadClass(threading.Thread):
	
	fetchall = None
	fetchone = None

	def __init__(self, conn):
		self.conn = conn
		threading.Thread.__init__(self)
			
	def run(self):
		self.fetchall = test_fetchall(self.conn)
		self.fetchone =  test_fetchone(self.conn)
	
		
class Test(unittest.TestCase):

	conn = None
	
	
	def setUp(self):
		pass


	def tearDown(self):
		pass


	@classmethod
	def setUpClass(cls):
		t = sqlite_server.SQLiteServerThread(get_connection)
		t.daemon = True
		t.start()
		wait_until(lambda: t.ready == True, sleep = 0.1)
		cls.connection = t.connection
	
	
	def testSingleThread(self):
		result = test_fetchall(self.connection)
		self.assertEqual(result, [(1,)])
		
		result = test_fetchone(self.connection)
		self.assertEqual(result, (1,))


	def testMultipleThreads(self):
		t = ThreadClass(self.connection)
		t.daemon = True
		t.start()
		t.join()
		self.assertEqual(t.fetchall, [(1,)])
		self.assertEqual(t.fetchone, (1,))
		

class DummyConnection(object):
	
	isolation_level = None
	
	def __call__(self):
		return self


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()
	