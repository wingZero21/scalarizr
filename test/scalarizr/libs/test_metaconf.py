'''
Created on Jul 1, 2010

@author: marat
'''
import unittest
from xml.etree import ElementTree
from scalarizr.libs.metaconf import Configuration
from scalarizr.util import init_tests
from cStringIO import StringIO
import time
import re

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
	
	_xml2 = """
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
	
	def test_ini(self):
		conf = Configuration("ini")
		conf.readfp(StringIO(self._ini))
		self.assertEqual(conf.get("general/server_id"), "40c2a5f1-57e3-4b80-9642-138ea8514fb1")
		self.assertEqual(len(conf.get_list("general/*")), 14)
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

	def test_xml(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		self.assertEqual(conf.get("Storage/Keyspaces/Keyspace[1]").strip(), "")
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
		
		col = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace1']/ColumnFamily[1]")
		self.assertTrue(len(col.keys()), 3)
		self.assertTrue("CompareWith" in col)
		self.assertEqual(col["CompareWith"], "UTF8Type")		
#		self.assertEqual(len(conf.get_list("Storage/Keyspaces/*")), 7)
	
	def test_subset(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))

		ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(ks_conf.get_dict("ColumnFamily[1]")["Name"], "Standard2")
	
	def test_parent(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(ks_conf._find("..").tag, 'Keyspaces')
		
	def test_set(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		conf.set("Storage/Keyspaces/Keyspace[@Name='Keyspace2']", dict(Keyword="TestKeyword", CompareWith="TestCompareWith"))
		attribs = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(attribs['Keyword'], "TestKeyword")
		self.assertEqual(attribs['CompareWith'], "TestCompareWith")

	def test_phpini_bench(self):
		start = time.time()
		conf = Configuration("ini")
		conf.readfp(open('/etc/php5/apache2/php.ini'))
		conf.set('PHP/memory_limit', '256')
		conf.set('MySQLi/mysqli.default_host', 'sql.trololo.net')
		conf.write(open('/home/spike/php.ini', 'w'))
		print "Php.ini time: ", time.time() - start
		
	def test_extend(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		conf.extend(StringIO(self._xml2))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
		conf.extend(StringIO(re.sub('UTF8Type', 'UTF16Type', self._xml2, 1)))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 3)
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
		conf.extend(StringIO(re.sub('"Keyspace2">', '"Keyspace2">some_text', self._xml2, 1)))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 4)
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 1)
		conf.extend(StringIO(re.sub('<Keyspaces>', '<Keyspaces Name="SomeName">', self._xml2, 1)))
		self.assertEqual(len(conf.get_list("Storage/Keyspaces")), 2)
		
		
		

if __name__ == "__main__":
	init_tests()
	unittest.main()