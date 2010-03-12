'''
Created on Mar 3, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.core.handlers import Handler
import logging
import os
import subprocess
import re

def get_handlers ():
	return [HooksHandler()]

class HooksHandler(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		Bus().on("init", self.on_init)
		
	def on_init(self):
		bus = Bus()
		for event in bus.list_events():
			bus.on(event, self.create_hook(event))
			
	def create_hook(self, event):
		def hook(*args, **kwargs):
			self._logger.info("Hook on '"+event+"'" + str(args) + " " + str(kwargs))

			bus = Bus()				
			config = bus[BusEntries.CONFIG]
			
			#for key in kwargs:
			#	os.environ[key] = kwargs[key]
			#os.environ["server_id"] = config.get("default", "server_id")
			#os.environ["behaviour"] = config.get("default", "behaviour")
			
			environ = kwargs
			environ["server_id"] = config.get("default", "server_id")
			environ["behaviour"] = config.get("default", "behaviour")
			
			path = bus[BusEntries.BASE_PATH] + "/hooks/"
			reg = re.compile(r"^\d+\-"+event+"$")
							
			if os.path.isdir(path):
				matches_list = list(fname for fname in os.listdir(path) if reg.search(fname))
				print matches_list
				if matches_list:
					matches_list.sort()
					for fname in matches_list:
						if os.access(path + fname, os.X_OK):	
							start_command = [path + fname]
							start_command += args
							try:
								p = subprocess.Popen(
									 start_command, 
									 stdin=subprocess.PIPE, 
									 stdout=subprocess.PIPE, 
									 stderr=subprocess.PIPE,
									 env=environ)								
								stdout, stderr = p.communicate()
							
								is_start_failed = p.poll()
								
								if is_start_failed:
									self._logger.error(stderr)
									
								if None != stdout:
									self._logger.info(stdout)	
							except OSError, e:
								self._logger.error(str(e.strerror) + ' in script ' + fname)			
		return hook