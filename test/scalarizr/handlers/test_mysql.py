'''
Created on 17.06.2010

@author: spike
'''

import unittest
import os
import signal, pexpect, re
from scalarizr.util import init_tests
from scalarizr.bus import bus
from scalarizr.handlers import mysql
from scalarizr.util import system, initd, disttool
from scalarizr.platform.ec2 import Ec2Platform
from subprocess import Popen, PIPE, STDOUT
import time, shutil, hashlib, pwd

class _MysqlHandler(mysql.MysqlHandler):
    def _init_storage(self):
        pass

class Test(unittest.TestCase):

    def setUp(self):
        system('cp -pr /etc/mysql/ /tmp/mysqletc/')
        system('cp -pr /var/lib/mysql /tmp/mysqldata/')

    def tearDown(self):
        initd.stop("mysql")
        system('cp /etc/mysql/my.cnf /tmp/etc')
        system('rm -rf /etc/mysql/')
        system('rm -rf /var/lib/mysql')
        system('cp -pr /tmp/mysqletc/ /etc/mysql/ ')
        system('cp -pr /tmp/mysqldata/ /var/lib/mysql ') 
        system('rm -rf /tmp/mysql*')
        initd.start("mysql")            

    def test_users(self):
        bus.queryenv_service = _QueryEnv()
        bus.platform = _Platform()
        handler = _MysqlHandler()
        root_user = "scalarizr"
        repl_user = "scalarizr_repl"
        stat_user = "scalarizr_stat"        
        root_password, repl_password, stat_password = handler._add_mysql_users(root_user, repl_user, stat_user)
        myd = handler._start_mysql_skip_grant_tables()
        for user, password in {root_user: root_password,
                               repl_user: repl_password,
                               stat_user: stat_password}.items():            
            myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
            out,err = myclient.communicate("SELECT Password from mysql.user where User='"+user+"'")
            hashed_pass = re.search ('Password\n(.*)', out).group(1)
            self.assertEqual(hashed_pass, mysql_password(password))
        os.kill(myd.pid, signal.SIGTERM)           
        
    def test_create_snapshot(self):
        bus.queryenv_service = _QueryEnv()
#        bus.platform = Ec2Platform()
        bus.platform = _Platform()
        handler = _MysqlHandler()
        root_user = "scalarizr"
        repl_user = "scalarizr_repl"
        stat_user = "scalarizr_stat"
        
        root_password, repl_password, stat_password = handler._add_mysql_users(root_user, repl_user, stat_user)
        handler._change_mysql_dir('log_bin', '/var/log/mysql/binarylog/binary.log', 'mysqld')
        handler._master_replication_init()
        snap_id, log_file, log_pos = handler._create_snapshot(root_user, root_password)
        sql = pexpect.spawn('/usr/bin/mysql -u' + root_user + ' -p' + root_password)
        sql.expect('mysql>')
        sql.sendline('SHOW MASTER STATUS;\n')
        sql.expect('mysql>')
        lines = sql.before
        # Retrieve log file and log position
        log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.MULTILINE), lines)
        if log_row:
            true_log_file = log_row.group(1)
            true_log_pos = log_row.group(2)
        sql.close()
        self.assertEqual(log_file, true_log_file)
        self.assertEqual(log_pos, true_log_pos)
        file = open('/etc/mysql/farm-replication.cnf')
        self.assertEqual('[mysqld]\nserver-id\t\t=\t1\nmaster-connect-retry\t\t=\t15\n', file.read())
        file.close()
        
    
    def test_on_before_host_init(self):
        bus.queryenv_service = _QueryEnv()
#        bus.platform = Ec2Platform()
        bus.platform = _Platform()
        handler = _MysqlHandler()
        message = _Message()
        handler.on_before_host_up(message)
        self.assertTrue(os.path.exists('/mnt/mysql-data'))
        self.assertTrue(os.path.exists('/mnt/mysql-misc'))
        mysql_user    = pwd.getpwnam("mysql")
        self.assertEqual(mysql_user.pw_uid, os.stat('/mnt/mysql-data')[4])
        self.assertEqual(mysql_user.pw_gid, os.stat('/mnt/mysql-data')[5])
        self.assertEqual(mysql_user.pw_uid, os.stat('/mnt/mysql-misc')[4])
        self.assertEqual(mysql_user.pw_gid, os.stat('/mnt/mysql-misc')[5])
        self.tearDown()
        self.setUp()
        handler.on_before_host_up(message)
        if disttool.is_redhat_based():
            my_cnf_file = "/etc/my.cnf"
        else:
            my_cnf_file = "/etc/mysql/my.cnf"
        file = open(my_cnf_file)
        mycnf = file.read()
        file.close()
        datadir = re.search(re.compile('^\s*datadir\s*=\s*(.*)$', re.MULTILINE), mycnf).group(1)
        self.assertEqual(datadir, '/mnt/mysql-data/')
        log_bin = re.search(re.compile('^\s*log_bin\s*=\s*(.*)$', re.MULTILINE), mycnf).group(1)
        self.assertEqual(log_bin, '/mnt/mysql-misc/binlog.log')

def mysql_password(str):
    pass1 = hashlib.sha1(str).digest()
    pass2 = hashlib.sha1(pass1).hexdigest()
    return "*" + pass2.upper()


class _Bunch(dict):
            __getattr__, __setattr__ = dict.get, dict.__setitem__

class _QueryEnv:
    def list_role_params(self, role_name):
        return _Bunch(
            mysql_data_storage_engine = 'ebs',           
            )
        
class _Platform:
    def get_instance_id(self):
        pass

class _Message:
    def __init__(self):
        self.log_file = None
        self.log_pos  = None
        self.mysql_repl_user = None
        self.mysql_repl_password = None
        self.mysql_stat_password = None
        self.mysql_stat_user = None

if __name__ == "__main__":
    init_tests()
    unittest.main()

