'''
Created on Mar 21, 2012

@author: dmitry
'''
import sys
import time
import threading
import sqlite3
import unittest
from scalarizr.util import sqlite_server


db = None

		
def get_connection():
	return sqlite3.Connection(database='/Users/dmitry/Documents/workspace/scalarizr-localobj/share/db.sql')


class ThreadClass(threading.Thread):
	
	def __init__(self):
		threading.Thread.__init__(self)
		
	def run(self):
		server = sqlite_server.SqliteServer(get_connection)
		global db
		db = server.connect()
		server.serve_forever()
		
		
class Test(unittest.TestCase):


	def setUp(self):
		pass

	def tearDown(self):
		pass


	def testSingleThread(self):
		
		t = ThreadClass()
		t.daemon = True
		t.start()
		time.sleep(1)
		
		cursor = db.cursor()
		cursor.execute('select 1;')
		result = cursor.fetchall()
		self.assertEqual(result, [(1,)])
		
		
		cursor = db.cursor()
		cursor.execute('select 1;')
		result = cursor.fetchone()
		self.assertEqual(result, (1,))


class DummyConnection(object):
	
	isolation_level = None
	
	def __call__(self):
		return self


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()
	