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
	base_path = bus[BusEntries.BASE_PATH]

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
	
	# Read behaviour configurations and inject them into global config
	from ConfigParser import ConfigParser
	from scalarizr.util import inject_config
	behaviour = config.get("default", "behaviour").split(",")
	for bh in behaviour:
		filename = "%s/etc/include/behaviour.%s.ini" % (base_path, bh)
		if os.path.exists(filename):
			logger.debug("Read behaviour configuration file %s", filename)
			bh_config = ConfigParser()
			bh_config.read(filename)
			inject_config(config, bh_config)
			
	
	# Run installation process
	if len(sys.argv) > 1 and sys.argv[1] == "--install":
		install()
	
	
	# Define scalarizr events
	bus.define_events(
		# Fires when starting
		"start",
		# Fires when terminating
		"terminate"
	)
	
	
	# Initialize platform
	logger.debug("Initialize platform")
	from scalarizr.platform import PlatformFactory 
	pl_factory = PlatformFactory()
	bus[BusEntries.PLATFORM] = pl_factory.new_platform(config.get("default", "platform"))

	
	# Initialize QueryEnv
	logger.debug("Initialize QueryEnv client")
	from scalarizr.core.queryenv import QueryEnvService
	crypto_key_path = base_path + "/" + config.get("default", "crypto_key_path")
	crypto_key = open(crypto_key_path).read()
	queryenv = QueryEnvService(config.get("default", "queryenv_url"),
			config.get("default", "server_id"), crypto_key)
	bus[BusEntries.QUERYENV_SERVICE] = queryenv

	
	# Initialize messaging
	logger.debug("Initialize messaging")
	from scalarizr.messaging import MessageServiceFactory
	factory = MessageServiceFactory()
	try:
		service = factory.new_service(config.get("messaging", "adapter"), config.items("messaging"))
		bus[BusEntries.MESSAGE_SERVICE] = service
	except Exception, e:
		logger.exception(e)
		sys.exit("Cannot create messaging service adapter '%s'" % (config.get("messaging", "adapter")))

	from scalarizr.core.handlers import MessageListener	
	consumer = service.get_consumer()
	consumer.add_message_listener(MessageListener())


	# Fire start
	bus.fire("start")


	# Start messaging server
	try:
		consumer.start()
	except KeyboardInterrupt:
		logger.info("Stopping scalarizr...")
		consumer.stop()
		
		# Fire terminate
		bus.fire("terminate")
		logger.info("Stopped")

	