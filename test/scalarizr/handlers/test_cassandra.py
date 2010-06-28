'''
Created on June, 25 2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.util import init_tests, system
from scalarizr.bus import bus
from scalarizr.handlers import cassandra
from scalarizr.util import fstool
from xml.dom.minidom import parse

class Test(unittest.TestCase):

	def setUp(self):
		config = bus.config
		self._storage_path = config.get('behaviour_cassandra','storage_path')
		self._storage_conf = config.get('behaviour_cassandra','storage_conf')
		
	def tearDown(self):
		#system("umount /dev/sdb1")
		fstool.umount("/dev/sdb1", clean_fstab = True)
		
	def testName(self):
		class _Bunch(dict):
			__getattr__, __setattr__ = dict.get, dict.__setitem__
			
		class _QueryEnv:
			def list_role_params(self, role_name=None):
				return _Bunch(cassandra_data_storage_engine = "eph"
			)
		bus.platform = _Platform()
		bus.queryenv_service = _QueryEnv()
		C = cassandra.CassandraHandler()
		C.on_before_host_up("")
		
		xml = parse(self._storage_conf)
		data = xml.documentElement
		
		log_entry = data.getElementsByTagName("CommitLogDirectory")
		self.assertEqual(log_entry[0].firstChild.nodeValue, C.commit_log_directory)
		
		data_entry = data.getElementsByTagName("DataFileDirectory")
		self.assertEqual(data_entry[0].firstChild.nodeValue, C.data_file_directory)
		
	def _test_fstab(self):
			fstab = fstool.Fstab("/etc/fstab")
			entries = fstab.list_entries()
			
			self.assertEqual(entries[1].device, "/dev/sda1")
			self.assertEqual(entries[1].mpoint, "/")
			self.assertEqual(entries[1].fstype, "ext4")
			self.assertEqual(entries[1].options, "errors=remount-ro")
			self.assertEqual(entries[1].value, "/dev/sda1       /               ext4    errors=remount-ro 0       1")
		
		

class _Platform:
	def get_block_device_mapping(self):
		return dict(ephemeral0 ="sdb1")

if __name__ == "__main__":
	init_tests()
	unittest.main()