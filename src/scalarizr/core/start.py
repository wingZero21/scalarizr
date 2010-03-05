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

	from scalarizr.core import Bus, BusEntries, initialize_services
	import logging	
	logger = logging.getLogger("scalarizr.core")
	bus = Bus()


	def install (argv=None):
		if argv is None:
			argv = sys.argv
			
		global config, logger, base_path
		logger.info("Running installation process")
			
		for pair in argv[2:]:
			pair = pair.split("=", 1)
			if pair[0].startswith("--"):
				key = pair[0][2:]
				value = pair[1] if len(pair) > 1 else None
	
				section_option = key.split(".")
				section = section_option[0] if len(section_option) > 1 else "default"
				option = section_option[1] if len(section_option) > 1 else section_option[0]
				if config.has_option(section, option):
					config.set(section, option, value)
				elif section == "default" and option == "crypto_key":
					# Update crypto key
					f = open(base_path + "/" + config.get("default", "crypto_key_path"), "w+")
					f.write(value)
					f.close()
					
		# Save configuration
		filename = Bus()[BusEntries.BASE_PATH] + "/etc/config.ini"
		logger.debug("Save configuration into '%s'" % filename)
		f = open(filename, "w")
		config.write(f)
		f.close()
	
	logger.info("Starting scalarizr...")
	
	
	# Run installation process
	if len(sys.argv) > 1 and sys.argv[1] == "--install":
		install()
	
	# Initialize services
	initialize_services()

	# Fire start
	bus.fire("start")

	# @todo start messaging before fire 'start'
	# Start messaging server
	try:
		consumer = bus[BusEntries.MESSAGE_SERVICE].get_consumer()
		consumer.start()
	except KeyboardInterrupt:
		logger.info("Stopping scalarizr...")
		consumer.stop()
		
		# Fire terminate
		bus.fire("terminate")
		logger.info("Stopped")
