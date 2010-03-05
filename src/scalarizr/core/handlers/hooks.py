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
			
			for key in kwargs:
				os.environ[key] = kwargs[key]
			
			bus = Bus()				
			config = bus[BusEntries.CONFIG]
			os.environ["server_id"] = config.get("default", "server_id")
			os.environ["behaviour"] = config.get("default", "behaviour")
			
			path = bus[BusEntries.BASE_PATH] + "/hooks/"
							
			if os.path.isdir(path):
				
				matches_list = []
				dir_list=os.listdir(path)
				for fname in dir_list:
					if (fname.startswith(event,3)) and ((fname.startswith(event+'.',3)) or (fname.endswith(event))):
						matches_list.append(fname)
				
				for fname in matches_list.sort():
					if os.access(path + fname, os.X_OK):	
						start_command = []
						start_command.append(path + fname) 
						for argument in args:
							start_command.append(argument) 
						
						p = subprocess.Popen(
							 start_command, 
							 stdin=subprocess.PIPE, 
							 stdout=subprocess.PIPE, 
							 stderr=subprocess.PIPE)
						
						stdout, stderr = p.communicate()
						is_start_failed = p.poll()
						
						if is_start_failed:
							self._logger.error(stderr)				
		return hook