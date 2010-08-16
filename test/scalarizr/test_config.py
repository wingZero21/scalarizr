'''
Created on Aug 11, 2010

@author: marat
'''
import unittest
from scalarizr.bus import bus
from scalarizr.config import Configurator, ScalarizrCnf, ScalarizrIni, split
from scalarizr.util import init_tests
from scalarizr.handlers import mysql
from scalarizr.handlers.apache import ApacheOptions


class TestOption(unittest.TestCase):
	class MyOption(Configurator.Option):
		name = 'test'
		default = 'fat'
	
	def test_auto(self):
		c = Configurator()
		o = self.MyOption()
		c.configure_option(o, 'thin', silent=True, yesall=True)
		self.assertEqual(o.value, 'thin')	

	def test_app(self):
		o = ApacheOptions.apache_conf_path()
		print o.default

class Test(unittest.TestCase):

	def setUp(self):
		bus.cnf = ScalarizrCnf()
		bus.cnf.bootstrap()
		
	def tearDown(self):
		del bus.cnf
	
	def test_state(self):
		print ScalarizrCnf.state
		cnf = ScalarizrCnf()
		print cnf.state
		cnf.state = 'running'
		print cnf.state
		'''
		print bus.cnf.state
		bus.cnf.state = 'running'
		print bus.cnf.state
		'''
		pass

	'''
	def test_validate(self):
		def on_error(o, e):
			print '[%s] %s' % (o.name, e)
		bus.cnf.validate(onerror=on_error)
	'''

	'''
	def test_ini_wrapper(self):
		ini = bus.cnf.ini
		print ini.general.behaviour
		#self.assertEqual(set('app', 'mysql', 'www'), set(ini.general.behaviour))
	'''
	
	
	'''
	def test_reconfigure_silent(self):
		values = {
			'www/binary_path' : '/usr/sbin/ntfsclone',
			'app/apache_config_path' : '/etc/euca2ools/eucarc',
			'general/crypto_key' : 'q9mBWijQrEehNSN77OiEHqA0r0U3PJb3ydvH2kkQz5wxqxpKfSFLGQ=='
		}
		bus.cnf.reconfigure(values, silent=True, yesall=True)

	def test_decorators(self):
		mysqld_path = mysql.MysqlOptions.mysqld_path()
		print mysqld_path.default
		print mysqld_path.default
		mysqld_path.value = '/usr/bin/python2.6'
		print mysqld_path.value
	'''

if __name__ == "__main__":
	init_tests()
	unittest.main()