'''
Created on Dec 16, 2009

@author: marat
'''

if __name__ == "__main__":

	from optparse import OptionParser
	import sys
	import os
	
	# Append src path to PYTHONPATH
	src_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
	sys.path.append(src_path)
	
	
	parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
	parser.add_option("-n", "--name", dest="name", help="Message name")
	parser.add_option("-q", "--queue", dest="queue", help="Queue to send message into")
	
	(options, args) = parser.parse_args()
	
	if options.queue is None or options.name is None:
		print parser.format_help()
		sys.exit()
	
	from scalarizr.core import Bus, BusEntries
	from scalarizr.messaging import MessageServiceFactory
	
	config = Bus()[BusEntries.CONFIG]
	factory = MessageServiceFactory()
	service = factory.new_service(config.get("messaging", "adapter"), config.items("messaging"))
	producer = service.get_producer()
	
	msg = service.new_message()
	msg.name = options.name
	for pair in args:
		k, v = pair.split("=")
		msg.body[k] = v
		
	producer.send(options.queue, msg)

	print "Done"