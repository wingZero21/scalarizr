'''
@author: Dmytro Korsakov
'''
import unittest
from scalarizr.util import init_tests
import os
from scalarizr.bus import bus
from scalarizr.handlers import ip_list_builder

class _Bunch(dict):
			__getattr__, __setattr__ = dict.get, dict.__setitem__

class _QueryEnv:
	def list_roles(self, role_name=None, behaviour=None):
		return [_Bunch(
			behaviour = "mysql",
			name = "mysql-lvm",
			hosts = [_Bunch(
				index='1',
				replication_master=True,
				internal_ip="127.0.0.1",
				external_ip="192.168.1.92"
				),
				_Bunch(
				index='2',
				replication_master=False,
				internal_ip="127.0.0.2",
				external_ip="192.168.1.93"
				)
				]
			)]
		
class Message:
	def __init__(self,body={}):
		self.body = body
	
class TestIpListBuilder(unittest.TestCase):
	
	def setUp(self):
		self.ip_lb = ip_list_builder.IpListBuilder()
		config = bus.config
		self.base_name = config.get('handler_ip_list_builder','base_path') 
		
	def test_host_is_replication_master(self):				
		is_replication_master = self.ip_lb._host_is_replication_master('127.0.0.1', 'mysql-lvm')
		self.assertTrue(is_replication_master)
		
	def _on_HostUpDown(self,internal_ip, prefix):
		role_alias = 'mysql'
		role_name = 'mysql-lvm'
		role_dir = self.base_name + os.sep + role_name
		role_file = role_dir + os.sep + internal_ip
		mysql_dir = self.base_name + os.sep + role_alias + prefix #
		mysql_file = mysql_dir + os.sep + internal_ip
		msg = Message(dict(RoleAlias=role_alias,InternalIP=internal_ip,RoleName=role_name))
		self.ip_lb.on_HostUp(msg)
		self.assertTrue(os.path.exists(role_dir) and os.path.isdir(role_dir))
		self.assertTrue(os.path.exists(role_file) and os.path.isfile(role_file))
		self.assertTrue(os.path.exists(mysql_dir) and os.path.isdir(mysql_dir))
		self.assertTrue(os.path.exists(mysql_file) and os.path.isfile(mysql_file))
		self.ip_lb.on_HostDown(msg)
		self.assertFalse(os.path.exists(role_dir))
		self.assertFalse(os.path.exists(role_file))
		self.assertFalse(os.path.exists(mysql_dir))
		self.assertFalse(os.path.exists(mysql_file))
	
	def test_on_HostUpDown1(self):
		self._on_HostUpDown(internal_ip = '127.0.0.1', prefix = "-master")

		
	def test_on_HostUpDown2(self):
		self._on_HostUpDown(internal_ip = '127.0.0.2', prefix = "-slave")

init_tests()
config = bus.config
config.read( os.path.realpath(os.path.dirname(__file__) + "/../../../etc/public.d/handler.ip_list_builder.ini"))
bus.queryenv_service = _QueryEnv()				
if __name__ == "__main__":
	unittest.main()