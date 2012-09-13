
import mock

import sys

eph_host_init_response_new_master = {
	'server_index': '1',
	'db_type': 'percona',
	'percona': {
		'replication_master': 1,
		'volume_config': {
			'type': 'eph',
		},
		'snapshot_config': None,
		'root_password': None,
		'repl_password': None,
		'stat_password': None,
		'log_file': None,
		'log_pos': None
	}
}

eph_host_init_response_slave = {
	'local_ip': '10.146.34.58',
	'remote_ip': '176.34.6.168',
	'db_type': 'percona',
	'percona': {
		'replication_master': 1,
		'volume_config': {
			'type': 'eph',
			'id': 'eph-vol-2a3bd1c8'
		},
		'snapshot_config': {
			'type': 'eph',
			'id': 'eph-snap-fe9ac9ce'			
		}
	}
}
							
eph_new_master_up = {
	'behaviour': ['percona'],
	'local_ip': '10.146.34.58',
	'remote_ip': '176.34.6.168',
	'db_type': 'percona',
	'percona': {
		'replication_master': '1',
		'snapshot_config': {'type': 'eph'},
		'root_password': 'zcuDiVum9hDvx1v97Ac5',
		'repl_password': 'cumLityXgnJv5JgaxmXA',
		'stat_password': 'UVVkn7ygMEMt7WktIqoy',
		'log_file': 'binlog.000003',
		'log_pos': '107'
	}
}

class NodeMock(dict):

	def __init__(self, *args, **kwds):
		super(NodeMock, self).__init__(
				percona={}, 
				behavior=['percona'])
	
	def __setitem__(self, key, value):
		if key == 'replication_master':
			value = int(key)
		super(NodeMock, self).__setitem__(key, value)


@mock.patch.multiple('scalarizr.handlers.mysql2', 
				bus=mock.DEFAULT, 
				mysql_svc=mock.DEFAULT,
				MysqlCnfController=mock.DEFAULT,
				ServiceCtlHandler=mock.DEFAULT)
@mock.patch.multiple('scalarizr.storage2', 
				volume=mock.DEFAULT,
				snapshot=mock.DEFAULT)
@mock.patch.multiple('scalarizr.services.backup', 
				backup=mock.DEFAULT,
				restore=mock.DEFAULT)
@mock.patch('scalarizr.node.__node__', NodeMock())
class TestMysqlHandler(object):
	
		
	def test_master_new(self, **kwds):
		from scalarizr.handlers import mysql2
		
		hdlr = mysql2.MysqlHandler()
		mock.patch.object(hdlr, '_storage_valid', return_value=False)
		
		msg = mock.Mock(**eph_host_init_response_new_master)
		hdlr.on_host_init_response(msg)
		hdlr.on_before_host_up(msg)
		
		__mysql__ = mysql2.__mysql__
		assert 'backup' in __mysql__
		assert __mysql__['backup'].type == 'snap_mysql'
		assert __mysql__['volume']
	
	def test_master_respawn(self, **kwds):
		pass
	
	
	def test_master_respawn_from_snapshot(self, **kwds):
		pass
	
	
	def test_slave(self, **kwds):
		pass


	def test_create_data_bundle(self, **kwds):
		pass
	
	
	def test_create_backup(self, **kwds):
		pass


	def test_slave_to_master(self, **kwds):
		pass
	
	
	def test_new_master_up(self, **kwds):
		pass
	
	
	
class TestMysqlHandlerXtrabackup(object):
	def test_master_new(self):
		pass
	
	def test_master_respawn(self):
		pass


class TestMysqlHandlerEphToLvm(object):
	def test_migrate_on_restart(self):
		pass
	
	def test_create_data_bundle(self):
		pass	
	
