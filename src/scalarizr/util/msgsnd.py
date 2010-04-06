'''
Created on Dec 16, 2009

@author: marat
'''
from scalarizr.util import configtool


def main():

	from optparse import OptionParser
	import sys
	
	parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
	parser.add_option("-n", "--name", dest="name", help="Message name")
	parser.add_option("-s", "--self", dest="self_send", default=True, help="Send message to myself")
	parser.add_option("-q", "--queue", dest="queue", help="Queue to send message into")
	
	(options, args) = parser.parse_args()
	
	if options.queue is None or options.name is None:
		print parser.format_help()
		sys.exit()
	
	from scalarizr.bus import Bus
	from scalarizr.messaging import MessageServiceFactory
	
	config = Bus().config
	adapter = config.get(configtool.SECT_MESSAGING, configtool.OPT_ADAPTER)
	factory = MessageServiceFactory()
	if options.self_send:
		producer_config = []
		for key, value in config.items(configtool.SECT_MESSAGING):
			if key.startswith(adapter + "_consumer"):
				producer_config.append((key.replace("consumer", "producer"), value))
		service = factory.new_service(adapter, producer_config)
	else:
		service = factory.new_service(adapter, config.items(configtool.SECT_MESSAGING))
		
	producer = service.get_producer()
	
	msg = service.new_message()
	msg.name = options.name
	for pair in args:
		k, v = pair.split("=")
		msg.body[k] = v
		
	producer.send(options.queue, msg)

	print "Done"