import logging, sys, os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..'))
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')

logging.basicConfig(
		format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
		stream=sys.stdout, 
		level=logging.DEBUG)	

