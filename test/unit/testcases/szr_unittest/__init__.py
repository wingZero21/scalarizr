
from scalarizr.bus import bus
from scalarizr import _db_connect, _create_db
from scalarizr.util import SqliteLocalObject

import logging, sys, os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..'))
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')
DB_SCRIPT = os.path.join(RESOURCE_PATH, 'db.sql')
DB_FILE = os.path.join(RESOURCE_PATH, 'db.sqlite')

def db_connection(file):
	def fn():
		return _db_connect(file)
	return SqliteLocalObject(fn)

def create_db(db_file=None, script_file=None):
	_create_db(db_file or DB_FILE, script_file or DB_SCRIPT)
	
def switch_db(db_file=None):
	bus.db = db_connection(db_file or DB_FILE)

def switch_reset_db(db_file=None):
	reset_db(db_file)	
	switch_db(db_file)
	
def reset_db(db_file=None):
	db_file = db_file or DB_FILE
	if os.path.exists(db_file):
		os.remove(db_file)
	create_db(db_file)


def main():
	# TODO: parse args
	logging.basicConfig(
			format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
			stream=sys.stdout, 
			level=logging.INFO)
