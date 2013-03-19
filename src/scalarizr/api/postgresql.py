'''
Created on Feb 25, 2013

@author: uty
'''


from scalarizr.util.cryptotool import pwgen
from scalarizr.services import postgresql as postgresql_svc
from scalarizr import rpc


class PostgreSQLAPI(object):

    @rpc.service_method
    def reset_password(self, new_password=None):
        """ Reset password for PostgreSQL user 'scalr'. Return new password """
        if not new_password:
            new_pass = pwgen(10)
        pg = postgresql_svc.PostgreSql()
        pg.root_user.change_role_password(new_pass)
        pg.root_user.change_system_password(new_pass)
        pg.reload()

        return new_pass
