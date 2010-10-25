import logging, sys, os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..'))
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')

logging.basicConfig(
		format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
		stream=sys.stdout, 
		level=logging.DEBUG)

import scalarizr
from scalarizr.util import *

def db_connect(file):
	import sqlite3 as sqlite
	
	def fn():
		conn = sqlite.connect(file, 5.0)
		conn.row_factory = sqlite.Row
		return conn	
	return SqliteLocalObject(fn)