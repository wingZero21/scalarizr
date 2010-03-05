#!/usr/bin/python
'''
Created on Mar 1, 2010

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

logger = logging.getLogger("scalarizr.scripts.udev")
bus = Bus()

logger.info("Starting udev script...")

try:
	initialize_scripts()

	msg_service = bus[BusEntries.MESSAGE_SERVICE]
	producer = msg_service.get_producer()

	msg = msg_service.new_message(Messages.BLOCK_DEVICE_UPDATED)
	for k, v in os.environ.items():
		msg.body[k.lower()] = v
	producer.send(Queues.CONTROL, msg)

except Exception, e:
	logger.exception(e)
