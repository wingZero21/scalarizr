#!/usr/bin/python
'''
Created on Mar 3, 2010

@author: marat
'''

import os
import sys

log = open("/var/log/scalarizr-reboot.log", "w+")
log.write("argv: " + str(sys.argv))

# Append src path to PYTHONPATH
my_path = os.path.realpath(__file__)
src_path = os.path.realpath(os.path.dirname(my_path) + "/../..")
sys.path.append(src_path)

from scalarizr.messaging import Messages, Queues
from scalarizr.core import Bus, BusEntries, initialize_scripts
import logging
try:
	import time
except ImportError:
	import timemodule as time

logger = logging.getLogger("scalarizr.scripts.reboot")
bus = Bus()	


logger.info("Starting reboot script...")

try:
	try:
		action = sys.argv[1]
	except IndexError:
		logger.error("Invalid execution parameters. argv[1] must be presented")
		sys.exit()
		
	if action == "start" or action == "stop":
		initialize_scripts()
			
		msg_service = bus[BusEntries.MESSAGE_SERVICE]
		producer = msg_service.get_producer()
		
		msg = msg_service.new_message(Messages.SERVER_REBOOT)
		producer.send(Queues.CONTROL, msg)
		
		# 30 seconds for termination
		start = time.time()
		while not msg.is_handled():
			if time.time() - start < 30:
				time.sleep(1)
			else:
				break
		
except (BaseException, Exception), e:
	log.write("Caught: " + str(sys.argv))
	logger.exception(e)
finally:
	log.close()