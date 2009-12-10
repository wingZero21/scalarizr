'''
Created on Dec 10, 2009

@author: marat
'''

if __name__ == "__main__":

	import sys
	import os.path
	
	# Append src path to PYTHONPATH
	src_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
	sys.path.append(src_path)
	
	from scalarizr.core import Bus, BusEntries
	import logging
	
	bus = Bus()
	config = bus[BusEntries.CONFIG]
	logger = logging.getLogger("scalarizr.core")
	
	logger.info("Starting scalarizr...")
	
	# Start messaging
	from scalarizr.messaging import MessageServiceFactory
	factory = MessageServiceFactory()
	try:
		service = factory.new_service(config.get("messaging", "adapter"), config.items("messaging"))
	except Exception, e:
		logger.exception(e)
		sys.exit("Cannot create messaging service adapter '%s'" % (config.get("messaging", "adapter")))
	
	producer = service.new_producer()
	bus[BusEntries.MESSAGE_PRODUCER] = producer
	
	consumer = service.new_consumer()
	bus[BusEntries.MESSAGE_CONSUMER] = consumer
	try:
		consumer.start()
	except KeyboardInterrupt:
		logger.info("Stopping scalarizr...")
		consumer.stop()
		logger.info("Stopped")
