
import os
import shutil
import json

from scalarizr.updclient import api as upd_api

from nose.tools import ok_, eq_
from nose.plugins.attrib import attr
import mock


SANDBOX_DIR = '/tmp/nosetests'
STATUS_FILE = SANDBOX_DIR + '/private.d/update.status'
CRYPTO_FILE = SANDBOX_DIR + '/private.d/keys/default'
SYSTEM_ID = '00000000-1111-2222-3333-000000000000'


class Thread(mock.Mock):
    def start(self):
        return self.target()


@mock.patch.multiple(upd_api, 
        queryenv=mock.DEFAULT, 
        metadata=mock.DEFAULT, 
        pkgmgr=mock.DEFAULT, 
        operation=mock.DEFAULT, 
        initdv2=mock.DEFAULT)
class TestUpdClientAPI(object):

    USER_DATA = {
        'behaviors': 'base,chef', 
        'farmid': '5071', 
        'message_format': 'json', 
        'owner_email': 'admin@scalr.net', 
        'szr_key': 'Y144+YKWk0l3Ukvf9FgjmSbALsLBS+Ujr70CmCkv', 
        's3bucket': '', 
        'cloud_server_id': '', 
        'env_id': '3414', 
        'server_index': '1', 
        'platform': 'ec2', 
        'role': 'base,chef', 
        'hash': '148840a0c2ab21', 
        'custom.scm_branch': 'feature/update-system', 
        'roleid': '36318', 
        'farm_roleid': '62025', 
        'serverid': 'b3e42c06-3f20-4cbe-8c1f-bc7c665ff975', 
        'p2p_producer_endpoint': 'https://my.scalr.com/messaging', 
        'realrolename': 'base-ubuntu1204-devel', 
        'region': 'us-east-1', 
        'httpproto': 'https', 
        'queryenv_url': 'https://my.scalr.com/query-env', 
        'cloud_storage_path': 's3://'
    }

    GLOBAL_CONFIG = {
        "scalr.version": "4.4.0",
        "scalr.id": "ab6d8171",
        "update.repository": "latest",
        "update.deb.repo_url": "http://apt.scalr.net/debian scalr/",
        "update.rpm.repo_url": "http://rpm.scalr.net/rpm/rhel/$releasever/$basearch",
        "update.win.repo_url": "http://win.scalr.net",
        #"update.server_url": "http://update.scalr.net/",
        #"update.client_mode": "client",
        #"update.api_port": "8008"
    }

    STATUS_FILE_1 = {
        'system_id': SYSTEM_ID,
        'package': 'scalarizr-ec2',
        'state': 'completed',
        'version': '0.21.26',
        'platform': USER_DATA['platform'],
        'queryenv_url': USER_DATA['queryenv_url'],
        'server_id': '9ff40b40-ac65-42c1-a1c6-24b882e96156',
    }

    STATUS_FILE_2 = {
        'system_id': 'd3775aa7-b17b-483b-9618-9acfb00f31be',
        'state': 'completed'
    }

    def setup(self):
        if not os.path.exists(SANDBOX_DIR):
            os.makedirs(SANDBOX_DIR)
        upd_api.UpdClientAPI.status_file = STATUS_FILE
        upd_api.UpdClientAPI.crypto_file = CRYPTO_FILE

    def teardown(self):
        if os.path.exists(SANDBOX_DIR):
            shutil.rmtree(SANDBOX_DIR)


    def setup_bootstrap(self, metadata=None, queryenv=None, status_file_data=None, mock_update=True, 
                mock_uninstall=False):
        upd = upd_api.UpdClientAPI()
        metadata.meta.return_value.user_data.return_value = self.USER_DATA.copy()
        queryenv.QueryEnvService.return_value.get_latest_version.return_value = '2013-11-21'
        if mock_update:
            mock.patch.object(upd, 'update', mock.DEFAULT).start()
        if mock_uninstall:
            mock.patch.object(upd, 'uninstall', mock.DEFAULT).start()
        mock.patch.object(upd, 'get_system_id', return_value=SYSTEM_ID).start()
        if status_file_data:
            if not os.path.exists(os.path.dirname(STATUS_FILE)):
                os.makedirs(os.path.dirname(STATUS_FILE))
            with open(STATUS_FILE, 'w+') as fp:
                json.dump(status_file_data, fp)
        return upd

    def assert_bootstrap_install(self, upd):
        eq_(upd.server_id, self.USER_DATA['serverid'])
        eq_(open(CRYPTO_FILE).read(), self.USER_DATA['szr_key'])
        eq_(upd.update.mock_calls, [mock.call(bootstrap=True)])


    def assert_bootstrap_no_install(self, upd, metadata):
        eq_(upd.server_id, self.STATUS_FILE_1['server_id'])
        ok_(not metadata.mock_calls)
        eq_(upd.update.mock_calls, [])


    def test_bootstrap_first_time(self, metadata=None, queryenv=None, pkgmgr=None, **kwds):
        upd = self.setup_bootstrap(metadata, queryenv)
        upd.bootstrap()
        self.assert_bootstrap_install(upd)

        eq_(queryenv.QueryEnvService.mock_calls, [
                mock.call.QueryEnvService(
                    self.USER_DATA['queryenv_url'], 
                    self.USER_DATA['serverid'], 
                    CRYPTO_FILE
                ), 
                mock.call().get_latest_version(),
                mock.call.QueryEnvService(
                    self.USER_DATA['queryenv_url'], 
                    self.USER_DATA['serverid'], 
                    CRYPTO_FILE,
                    api_version='2013-11-21'
                )
                ])
        ok_(not upd.system_matches)
        

    def test_bootstrap_after_restart_or_reboot(self, queryenv=None, metadata=None, **kwds):
        upd = self.setup_bootstrap(metadata, queryenv, status_file_data=self.STATUS_FILE_1)
        upd.bootstrap()
        self.assert_bootstrap_no_install(upd, metadata)
        ok_(upd.system_matches)


    def test_bootstrap_hardware_changed(self, queryenv=None, metadata=None, **kwds):
        upd = self.setup_bootstrap(metadata, queryenv, status_file_data=self.STATUS_FILE_2)
        upd.bootstrap()
        self.assert_bootstrap_install(upd)
        ok_(not upd.system_matches)


    def setup_sync(self, queryenv=None, metadata=None, **kwds):
        queryenv.QueryEnvService.return_value.get_global_config.return_value = {
            'params': self.GLOBAL_CONFIG
        }   
        upd = self.setup_bootstrap(metadata, queryenv, status_file_data=self.STATUS_FILE_1, **kwds)
        upd.bootstrap()
        return upd

    def test_sync(self, queryenv=None, metadata=None, pkgmgr=None, **kwds):
        upd = self.setup_sync(queryenv, metadata)
        upd.sync()

        eq_(upd.scalr_id, self.GLOBAL_CONFIG['scalr.id'])
        eq_(upd.repository, self.GLOBAL_CONFIG['update.repository'])
        eq_(upd.repo_url, self.GLOBAL_CONFIG['update.deb.repo_url'])
        eq_(pkgmgr.repository.mock_calls, [
                mock.call(upd.repository, upd.repo_url), 
                mock.call().ensure()
                ])


    def test_update(self, queryenv=None, metadata=None, **kwds):
        upd = self.setup_sync(queryenv, metadata, mock_update=False, mock_uninstall=True)
        candidate = '0.21.32'
        def run(*args, **kwds):
            return args[1](mock.Mock())
        upd.op_api.run.side_effect = run
        upd.pkgmgr.info.return_value = {'candidate': candidate}
        mock.patch.multiple(upd, update_server=mock.DEFAULT, scalarizr=mock.DEFAULT).start()
        upd.update_server.update_allowed.return_value = True
        upd.scalarizr.operation.has_in_progress.return_value = False

        upd.update()

        ok_(not upd.uninstall.called)
        ok_(upd.scalarizr.operation.has_in_progress.called)
        eq_(upd.pkgmgr.install.mock_calls, [mock.call(upd.package, candidate)])
        with open(STATUS_FILE) as fp:
            lock_data = json.load(fp)
            eq_(lock_data['version'], candidate)

