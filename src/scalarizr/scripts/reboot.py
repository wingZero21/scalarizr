#!/usr/bin/python
'''
Created on Mar 3, 2010

@author: marat
'''

import sys

log = open("/var/log/scalarizr-reboot.log", "w+")
log.write("argv: " + str(sys.argv))


from scalarizr.messaging import Messages, Queues
from scalarizr.core import Bus, BusEntries, init_scripts
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
		init_scripts()
			
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