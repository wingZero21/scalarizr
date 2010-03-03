#!/usr/bin/python
'''
Created on Mar 1, 2010

@author: marat
'''

import os
import sys
import logging

# Append src path to PYTHONPATH
src_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
sys.path.append(src_path)

from scalarizr.messaging import MessageServiceFactory, Messages, Queues
from scalarizr.core import Bus, BusEntries

logger = logging.getLogger("scalarizr.core.udev")

try:
	config = Bus()[BusEntries.CONFIG]
	factory = MessageServiceFactory()

	adapter = config.get("messaging", "adapter")
	
	# Make producer config from consumer
	producer_config = []
	for key, value in config.items("messaging"):
		if key.startswith(adapter + "_consumer"):
			producer_config.append((key.replace("consumer", "producer"), value))

	service = factory.new_service(adapter, producer_config)
	producer = service.get_producer()

	msg = service.new_message(Messages.BLOCK_DEVICE_UPDATED)
	for k, v in os.environ.items():
		msg.body[k.lower()] = v
	producer.send(Queues.CONTROL, msg)

except Exception, e:
	logger.fatal("Cannot send udev event to scalarizr. %s" % str(e))
