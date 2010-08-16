'''
Created on Apr 6, 2010

@author: marat
'''

from scalarizr.util import Observable
from optparse import OptionParser

class Bus(Observable):
	base_path = None
	"""
	@ivar string: Application base path
	"""
	
	etc_path = None
	"""
	@ivar string: Application etc path 
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
	@ivar sqlalchemy.pool.SingletonThreadPool: Database connection pool
	"""
	
	messaging_service = None
	"""
	@ivar scalarizr.messaging.MessageService: Default message service 
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

bus = Bus()