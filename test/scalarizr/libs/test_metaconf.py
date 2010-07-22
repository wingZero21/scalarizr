'''
Created on Jul 1, 2010

@author: marat
'''
import unittest
from scalarizr.libs.metaconf import Configuration
from cStringIO import StringIO
import time
import re, os

RESOURCE_PATH = os.path.abspath(os.path.dirname(__file__) + "../../../resources/libs/metaconf")


class PhpIniTest(unittest.TestCase):
	conf = None
	def setUp(self):
		self.conf = Configuration("ini")
		self.conf.read(RESOURCE_PATH + "/php.ini")
		
	def tearDown(self):
		del self.conf

	def test_get(self):
		self.assertEqual(self.conf.get("PHP/error_reporting"), "E_ALL & ~E_NOTICE")
		self.assertEqual(self.conf.get("PHP/error_log"), "/var/log/php_error.log")
		self.assertEqual(self.conf.get("PHP/include_path"), ".:/php/includes")
		self.assertEqual(self.conf.get("PHP/auto_append_file"), "")

	def test_get_boolean1(self):
		self.assertEqual(self.conf.get("Tidy/tidy.clean_output"), "Off")
		self.assertFalse(self.conf.get_boolean("Tidy/tidy.clean_output"))
		
	def test_get_boolean2(self):
		self.assertEqual(self.conf.get("MSSQL/mssql.allow_persistent"), "On")
		self.assertTrue(self.conf.get_boolean("MSSQL/mssql.allow_persistent"))

	
	def test_get_int(self):
		self.assertEqual(self.conf.get_int("PHP/post_max_size"), 8)

	extensions = ("curl.so", "dom.so", "http.so", "imap.so", "json.so", "mcrypt.so", "mongo.so", "mysqli.so")
	
	def test_get_list(self):
		self.assertEqual(set(self.extensions), set(self.conf.get_list("PHP/extension")))

	def test_get_list_order(self):
		self.assertEqual(self.extensions, tuple(self.conf.get_list("PHP/extension")))

	def test_comments(self):
		self.assertEqual(self.conf.get("PHP/memory_limit"), "128M")
		pass
	
	def test_set(self):
		self.conf.set("MSSQL/mssql.min_message_severity", "10")
		self.assertEqual(self.conf.get_int("MSSQL/mssql.min_message_severity"), 10)
	
	def test_set_list(self):
		self.conf.add("PHP/extension", "mysql.so")
		ext = list(self.extensions)
		ext.append("mysql.so")
		self.assertEqual(ext, self.conf.get_list("PHP/extension"))


class XmlTest(unittest.TestCase):
	conf = None		

	def setUp(self):
		self.conf = Configuration("xml")
		self.conf.read(RESOURCE_PATH + "/php.ini")
		
	def tearDown(self):
		del self.conf
		
	def test_get(self):
		self.assertEqual(self.conf.get("Storage/Authenticator"), "org.apache.cassandra.auth.AllowAllAuthenticator")
		
	def test_get2(self):
		self.assertEqual(self.conf.get("//Seeds/Seed[2]"), "192.168.1.200")

	def test_get_boolean(self):
		self.assertFalse(self.conf.get("Storage/AutoBootstrap"))
	
	def test_get_boolean2(self):
		self.assertTrue(self.conf.get("Storage/HintedHandoffEnabled"))
	
	def test_get_list(self):
		seeds = list("127.0.0.1", "192.168.1.200")
		self.assertEqual(seeds, self.conf.get_list("//Seeds/Seed"))
	
	def test_get_dict(self):
		col = self.conf.get_dict("//KeySpace/ColumnFamily Name='Standard2'")
		self.assertEqual(col["CompareWith"], "UTF8Type")
		self.assertEqual(col["KeysCached"], "100%")
		self.assertEqual(len(col.items), 3)
	

class Test(unittest.TestCase):

	
	_ini = """
[general]
; Unique server identificator in Scalr envirounment
server_id = 40c2a5f1-57e3-4b80-9642-138ea8514fb1


; Server behaviour (app|www|mysql) 
behaviour = app
behaviour = www
behaviour = mysql

; Role name ex = mysqllvm64
role_name = scalarizr-first
	"""
	
	_xml = """
<Storage>
  <Keyspaces>
    <Keyspace Name="Keyspace1">
    	<ColumnFamily Name="Standard2" 
                    CompareWith="UTF8Type"
                    KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>
    <Keyspace Name="Keyspace2">
    	<ColumnFamily Name="Standard2" 
                    CompareWith="UTF8Type"
                    KeysCached="100%"/>
        <ColumnFamily Name="StandardByUUID1" CompareWith="TimeUUIDType" />
    </Keyspace>    
  </Keyspaces>	
</Storage>
	"""

	
	def _test_php_ini(self):
		conf = Configuration("ini")
		conf.read(RESOURCE_PATH + "/php.ini")
		#self.assertEqual(conf.get_int("PHP/memory_limit"), 128)
		self.assertEqual(conf.get("PHP/error_reporting"), "E_ALL & ~E_NOTICE")
		
		
		
		"""
		conf.readfp(StringIO(self._ini))
		self.assertEqual(conf.get("general/server_id"), "40c2a5f1-57e3-4b80-9642-138ea8514fb1")
		self.assertEqual(len(conf.get_list("general/*")), 12)
		conf.add('general/behaviour', 'cassandra')
		self.assertEqual(conf.get_list("general/behaviour")[-1].strip(), 'cassandra')
		conf.add('general/some_param', 'some_value', before_path='server_id')
		self.assertEqual(conf.get_list("general/*")[3].strip(), 'some_value')
		conf.set('general/behaviour[1]', '1', typecast=float)
		self.assertEqual(conf.get("general/behaviour[1]"), '1.0')
		conf.remove('general/behaviour', 'www')
		self.assertEqual(len(conf.get_list("general/behaviour")), 3)
		conf.remove('general/behaviour')
		self.assertEqual(len(conf.get_list("general/behaviour")), 0)
		"""

	def _test_xml(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		self.assertEqual(conf.get("Storage/Keyspaces/Keyspace[1]").strip(), "")
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
		
		col = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace1']/ColumnFamily[1]")
		self.assertTrue(len(col.keys()), 3)
		self.assertTrue("CompareWith" in col)
		self.assertEqual(col["CompareWith"], "UTF8Type")		
#		self.assertEqual(len(conf.get_list("Storage/Keyspaces/*")), 7)
	
	def _test_subset(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))

		ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(ks_conf.get_dict("ColumnFamily[1]")["Name"], "Standard2")
	
	def _test_parent(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(ks_conf._find("..").tag, 'Keyspaces')
		
	def _test_set(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		conf.set("Storage/Keyspaces/Keyspace[@Name='Keyspace2']", dict(Keyword="TestKeyword", CompareWith="TestCompareWith"))
		attribs = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(attribs['Keyword'], "TestKeyword")
		self.assertEqual(attribs['CompareWith'], "TestCompareWith")

	def _test_phpini_bench(self):
		start = time.time()
		conf = Configuration("ini")
		if os.path.exists('/etc/php5/apache2/php.ini'):
			conf.readfp(open('/etc/php5/apache2/php.ini'))
		else:
			conf.readfp(open('/etc/php.ini'))
		conf.set('PHP/memory_limit', '256')
		conf.set('MySQLi/mysqli.default_host', 'sql.trololo.net')
		conf.write(open('/home/spike/php.ini', 'w'))
		print "Php.ini time: ", time.time() - start
		
	def _test_extend(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
		conf.readfp(StringIO(re.sub('UTF8Type', 'UTF16Type', self._xml, 1)))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 3)
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
		conf.readfp(StringIO(re.sub('"Keyspace2">', '"Keyspace2">some_text', self._xml, 1)))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 4)
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
		conf.readfp(StringIO(re.sub('<Keyspaces>', '<Keyspaces Name="SomeName">', self._xml, 1)))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 2)
		
	def _test_nginx(self):
		conf = Configuration("nginx")
		conf.read('/etc/nginx/nginx.conf')
		self.assertEqual(conf.get_int('worker_processes'), 1)
		conf.set('user', 'www www')	
		self.assertEqual(conf.get('user'),'www www')

if __name__ == "__main__":
	#init_tests()
	unittest.main()