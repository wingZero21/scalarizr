'''
Created on Apr 6, 2010

@author: marat
'''

from scalarizr.libs.pubsub import Observable

class _Bus(Observable):
	base_path = None
	"""
	@ivar string: Application base path
	"""
	
	etc_path = None
	"""
	@ivar string: etc path 
	"""
	
	share_path = None
	'''
	@ivar string: Shared resources path
	'''
	
	scalr_url = None
	"""
	@ivar string: Base URL to Scalr service
	"""
	
	cnf = None
	'''
	@ivar cnf: Scalarizr configuration facade. Access ini settings, read/write keys
	@type cnf: scalarizr.config.ScalarizrCnf 
	'''
	
	config = None
	"""
	@ivar ConfigParser.ConfigParser: Configuration (config.ini and includes) 
	"""
	
	optparser = None
	"""
	@ivar optparse.OptionParser: Command line options
	"""
	
	db = None
	"""
	@ivar db: Database connection pool. Single connection per thread
	@type db: scalarizr.util.SqliteLocalObject
	"""
	
	messaging_service = None
	"""
	@ivar messaging_service: Default message service (Scalarizr <-> Scalr) 
	@type messaging_service: scalarizr.messaging.MessageService
	"""
	
	int_messaging_service = None
	"""
	@ivar int_messaging_service: Cross-scalarizr internal messaging
	@type int_messaging_service: scalarizr.handlers.lifecircle.IntMessagingService
	"""
	
	queryenv_service = None
	"""
	@ivar scalarizr.queryenv.QueryEnv:  QueryEnv service client
	"""
	
	snmp_server = None
	"""
	@ivar scalarizr.snmpagent.SnmpServer: SNMP embed server
	"""
	
	platform = None
	"""
	@ivar scalarizr.platform.Platform: Platform (ec2, rs, vps...)
	"""
	
	periodical_executor = None
	'''
	@ivar: scalarizr.util.PeriodicalExecutor
	'''

bus = _Bus()
