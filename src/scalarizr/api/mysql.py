'''
Created on Dec 04, 2011

@author: marat
'''
from __future__ import with_statement

import string

from scalarizr import rpc, storage2
from scalarizr.api import operation
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import ServiceError
from scalarizr.services.mysql2 import __mysql__
from scalarizr.util.cryptotool import pwgen


class MySQLAPI(object):
    """
    @xxx: reporting is a pain
    """

    error_messages = {
        'empty': "'%s' can't be blank",
        'invalid': "'%s' is invalid, '%s' expected"
    }

    def __init__(self):
        self._mysql_init = mysql_svc.MysqlInitScript()
        self._op_api = operation.OperationAPI()

    @rpc.command_method
    def grow_volume(self, volume, growth, async=False):
        self._check_invalid(volume, 'volume', dict)
        self._check_empty(volume.get('id'), 'volume.id')

        def do_grow(op):
            vol = storage2.volume(volume)
            self._mysql_init.stop('Growing data volume')
            try:
                growed_vol = vol.grow(**growth)
                __mysql__['volume'] = dict(growed_vol)
                return dict(growed_vol)
            finally:
                self._mysql_init.start()

        return self._op_api.run('mysql.grow-volume', do_grow, async=async)


    def _check_invalid(self, param, name, type_):
        assert isinstance(param, type_), \
            self.error_messages['invalid'] % (name, type_)

    def _check_empty(self, param, name):
        assert param, self.error_messages['empty'] % name

    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Reset password for MySQL user 'scalr_master'. Return new password
        """
        if not new_password:
            new_password = pwgen(20)
        mysql_cli = mysql_svc.MySQLClient(__mysql__['root_user'],
                                          __mysql__['root_password'])
        master_user = __mysql__['master_user']

        if mysql_cli.user_exists(master_user, 'localhost'):
            mysql_cli.set_user_password(master_user, 'localhost', new_password)
        else:
            mysql_cli.create_user(master_user, 'localhost', new_password)

        if mysql_cli.user_exists(master_user, '%'):
            mysql_cli.set_user_password(master_user, '%', new_password)
        else:
            mysql_cli.create_user(master_user, '%', new_password)

        mysql_cli.flush_privileges()

        return new_password

    @rpc.query_method
    def replication_status(self):
        mysql_cli = mysql_svc.MySQLClient(__mysql__['root_user'],
                                          __mysql__['root_password'])
        if int(__mysql__['replication_master']):
            master_status = mysql_cli.master_status()
            result = {'master': {'status': 'up',
                                 'log_file': master_status[0],
                                 'log_pos': master_status[1]}}
            return result
        else:
            try:
                slave_status = mysql_cli.slave_status()
                slave_status = dict(zip(map(string.lower, slave_status.keys()),
                                        slave_status.values()))
                slave_running = slave_status['slave_io_running'] == 'Yes' and \
                    slave_status['slave_sql_running'] == 'Yes'
                slave_status['status'] = 'up' if slave_running else 'down'
                return {'slave': slave_status}
            except ServiceError:
                return {'slave': {'status': 'down'}}
