#!/usr/bin/python
'''
Created on Mar 3, 2010

@author: marat
'''

import os
import sys

# Append src path to PYTHONPATH
my_path = os.path.realpath(__file__)
src_path = os.path.realpath(os.path.dirname(my_path) + "/../..")
sys.path.append(src_path)

from scalarizr.messaging import Messages, Queues
from scalarizr.core import Bus, BusEntries, initialize_scripts
import logging

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
		
except (BaseException, Exception), e:
	logger.exception(e)
