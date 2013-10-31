
from nose.tools import eq_, ok_
import mock

@mock.patch.dict('scalarizr.node.__node__', {
	'messaging': mock.MagicMock(),
	'platform': 'ec2',
	'platform_obj': mock.Mock(),
	'behavior': 'percona',
	'mysql': {
		'behavior': 'percona'
	}
})
@mock.patch('scalarizr.services.backup.backup')
class TestMySQLAPI(object):

	def test_create_backup(self, bak, *args):
		backup_result =  {'chunk.1' : 'in clouds'}
		bak.return_value.run.return_value = backup_result
		
		from scalarizr.api.mysql import MySQLAPI
		api = MySQLAPI()
		api.create_backup(async=False)

		from scalarizr.node import __node__
		eq_(__node__['messaging'].send.call_args_list[0], 
			mock.call('DbMsr_CreateBackupResult', {'status': 'ok', 'backup_parts': backup_result, 'db_type': 'percona'}))
		args, kwds = __node__['messaging'].send.call_args_list[1]
		eq_(kwds['body']['result'], backup_result)
		