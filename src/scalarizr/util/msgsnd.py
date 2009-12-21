'''
Created on Dec 16, 2009

@author: marat
'''

if __name__ == "__main__":

	from optparse import OptionParser
	import sys
	
	parser = OptionParser(usage="Usage: %prog [options]")
	parser.add_option("-n", "--name", dest="name", help="Message name")
	parser.add_option("-q", "--queue", dest="queue", help="Queue to send message into")
	parser.add_option("-p", "--endpoint", dest="endpoint", help="Message server URL")
	
	
	(options, args) = parser.parse_args()

	if options.queue is None or options.name is None or options.endpoint is None:
		print parser.format_help()
		sys.exit() 
	
	print args
	print options