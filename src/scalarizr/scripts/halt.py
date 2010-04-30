'''
Created on Mar 3, 2010

@author: marat
'''


from scalarizr.messaging import Messages, Queues
from scalarizr.bus import bus
from scalarizr import init_script
import logging
import sys
try:
	import time
except ImportError:
	import timemodule as time


def main():
	logger = logging.getLogger("scalarizr.scripts.halt")
	
	logger.info("Starting halt script...")
	
	try:
		try:
			action = sys.argv[1]
		except IndexError:
			logger.error("Invalid execution parameters. argv[1] must be presented")
			sys.exit()
			
		if action == "start" or action == "stop":
			init_script()	
				
			msg_service = bus.messaging_service
			producer = msg_service.get_producer()
			
			msg = msg_service.new_message(Messages.SERVER_HALT)
			producer.send(Queues.CONTROL, msg)
			
			# 30 seconds for termination
			start = time.time()
			while not msg.is_handled():
				if time.time() - start < 30:
					time.sleep(1)
				else:
					break
				
	except (BaseException, Exception), e:
		logger.exception(e)
		
