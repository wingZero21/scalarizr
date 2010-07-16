'''
Created on May 7, 2010

@author: marat
'''
import unittest
import os
import sqlite3 as sqlite
from scalarizr.util import init_tests


class Test(unittest.TestCase):


	def _test_create_database(self):
		file = os.path.join(os.path.dirname(__file__), "../resources/db.sqlite")
		script_file = os.path.join(os.path.dirname(__file__), "../../etc/public.d/db.sql")
		if os.path.exists(file):
			os.remove(file) 
		
		conn = sqlite.Connection(file)
		conn.executescript(open(script_file).read())
		conn.commit()
		conn.close()
		


if __name__ == "__main__":
	init_tests()
	unittest.main()