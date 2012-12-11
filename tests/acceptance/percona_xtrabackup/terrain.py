import logging

logging.basicConfig(
		stream=open('/var/log/scalarizr.lettuce.log', 'w'), 
		level=logging.DEBUG)