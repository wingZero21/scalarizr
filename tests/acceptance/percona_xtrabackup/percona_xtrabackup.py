"""
:requires: env[AWS_ACCESS_KEY_ID] env[AWS_SECRET_ACCESS_KEY]
:requires: role[percona] | role[mysql2]
"""

import scalarizr.node
from scalarizr.services import backup
import scalarizr.services.mysql as mysql_svc

from lettuce import step


@step(u'Given i have running Percona server')
def given_i_have_running_percona_server(step):
	mysql_init = mysql_svc.MysqlInitScript()
	if not mysql_init.running:
		mysql_init.start()

@step(u'When i create full xtrabackup')
def when_i_create_full_xtrabackup(step):
	import scalarizr.services.mysql2
	bak = backup.backup(type='xtrabackup', 
			cloudfs_dest='s3://scalr.test_bucket/percona-xtrabackup-streaming')
	restore = bak.run()
	lettuce.world.restore = dict(restore)

@step(u'Then i have a restore object')
def then_i_have_a_restore_object(step):
	assert hasattr(lettuce.world, 'restore')
	restore = lettuce.world.restore
	assert restore['type'] == 'xtrabackup'
	assert restore['cloudfs_src']

@step(u'And cloudfs_src points to valid manifest')
def and_cloudfs_src_points_to_valid_manifest(step):
	restore = lettuce.world.restore
	assert restore['cloudfs_dest'].endswith('.json')
