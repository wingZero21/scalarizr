'''
Created on Jul 1, 2010

@author: marat
'''
import unittest
from xml.etree import ElementTree
from scalarizr.libs.metaconf import Configuration
from scalarizr.util import init_tests
from cStringIO import StringIO

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
	
	def test_ini(self):
		conf = Configuration("ini")
		conf.readfp(StringIO(self._ini))
		self.assertEqual(conf.get("general/server_id"), "40c2a5f1-57e3-4b80-9642-138ea8514fb1")

	def test_xml(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))
		self.assertEqual(conf.get("Storage/Keyspaces/Keyspace[1]").strip(), "")
		self.assertEqual(len(conf.get_list("Storage/Keyspaces/Keyspace")), 2)
		
		col = conf.get_dict("Storage/Keyspaces/Keyspace[@Name='Keyspace1']/ColumnFamily[1]")
		self.assertTrue(len(col.keys()), 3)
		self.assertTrue("CompareWith" in col)
		self.assertEqual(col["CompareWith"], "UTF8Type")

	def test_subset(self):
		conf = Configuration("xml")
		conf.readfp(StringIO(self._xml))

		ks_conf = conf.subset("Storage/Keyspaces/Keyspace[@Name='Keyspace2']")
		self.assertEqual(ks_conf.get_dict("ColumnFamily[1]")["Name"], "Standard2")
		pass

if __name__ == "__main__":
	init_tests()
	unittest.main()