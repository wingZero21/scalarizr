'''
Created on Jun 17, 2010

@author: marat
'''
from scalarizr.util import UtilError, system
import time
import os

class InitdError(UtilError):
	output = None
	def __init__(self, *args, **kwargs):
		UtilError.__init__(self, *args)
		if kwargs.has_key("output"):
			self.output = kwargs["output"]


_services = dict()

def explore(name, initd_script, pid_file=None, lock_file=None):
	_services[name] = dict(initd_script=initd_script, pid_file=pid_file, lock_file=lock_file)

def start(name):
	return _start_stop_reload(name, "start")

def stop(name): 
	return _start_stop_reload(name, "stop")
	
def reload(name, force=False): 
	return _start_stop_reload(name, "force-reload" if force else "reload")

def _start_stop_reload(name, action):
	if not _services.has_key(name):
		raise InitdError("Unknown service '%s'" % (name,))
	cmd = [_services[name]["initd_script"], action]
	out, err, retcode = system(cmd, shell=False)
	if retcode or (out and out.find("FAILED") != -1):
		raise InitdError("Cannot %s %s" % (action, name), output=out + " " + err)
	
	pid_file = _services[name]["pid_file"]
	
	# 1. on start init.d scripts often returns control right after daemon is forked 
	# but pid-file is not touched
	# 2. when doing apache reload
	if action != "stop":
		time.sleep(0.5)
	
	if pid_file:
		if action == "start" and not os.path.exists(pid_file):
			raise InitdError("Cannot start %s. pid file %s doesn't exists" % (name, pid_file))
		if action == "stop" and os.path.exists(pid_file):
			raise InitdError("Cannot stop %s. pid file %s still exists" % (name, pid_file))
		
	return True

def is_running(name):
	if not _services.has_key(name):
		raise InitdError("Unknown service '%s'" % (name,))
	cmd = [_services[name]["initd_script"], "status"]
	out, err = system(cmd, shell=False)[0:2]
	out += err
	return out.lower().find("running") != -1
