'''
Created on Dec 16, 2009

@author: marat
'''
from scalarizr import init_script
from scalarizr.bus import bus


def main():

	from optparse import OptionParser
	import sys
	
	parser = OptionParser(usage="Usage: %prog [options] key=value key2=value2 ...")
	parser.add_option("-n", "--name", dest="name", help="Message name")
	parser.add_option("-e", "--endpoint", dest="endpoint", default=None, help="Messaging server URL")
	parser.add_option("-q", "--queue", dest="queue", help="Queue to send message into")
	
	(options, args) = parser.parse_args()
	
	if options.queue is None or options.name is None:
		print parser.format_help()
		sys.exit()
	
	
	init_script()
	
	msg_service = bus.messaging_service
	producer = msg_service.get_producer()
	if options.endpoint:
		producer.endpoint = options.endpoint
	
	msg = msg_service.new_message()
	msg.name = options.name
	for pair in args:
		k, v = pair.split("=")
		msg.body[k] = v
		
	producer.send(options.queue, msg)

	print "Done"