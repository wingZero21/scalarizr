'''
Created on Oct 20, 2011

@author: ubuntu
'''
import unittest

from scalarizr.util import szradm

from scalarizr import queryenv


class LEM(szradm.ListEbsMountpointsCommand):
	def run(self):
		m1=queryenv.Mountpoint(name='Mountpoint 1', dir='dir1', create_fs=False, is_array=True,
			volumes=[queryenv.Volume(device='dev 1',volume_id='21'),queryenv.Volume(device='dev 2', volume_id='22')])

		m2=queryenv.Mountpoint(name='Mountpoint 2', dir='dir2', create_fs=False, is_array=True,
			volumes=[queryenv.Volume(device='dev 3',volume_id='23'),queryenv.Volume(device='dev 4', volume_id='24')])
		self.output([m1,m2])

class LRC(szradm.ListRolesCommand):
	def run(self):
		'''
		res=queryenv.Role(behaviour='mysql', name='mysql-ubuntu1004-trunk',
					hosts=queryenv.RoleHost(index='1', replication_master='1',
					internal_ip="10.242.75.80", external_ip='50.17.99.58'))
		'''

		res=[queryenv.Role(behaviour='mysql', name='mysql-ubuntu1004-trunk',
					hosts=queryenv.RoleHost(index='1', replication_master='1',
					internal_ip="10.242.75.80", external_ip='50.17.99.58')),
				queryenv.Role(behaviour='cf_router', name='cf-router64-ubuntu1004',
					hosts=queryenv.RoleHost(index='1', replication_master='1')),
				queryenv.Role(behaviour='www,cf_router,cf_cloud_coller,cfanager,cf_dea,cf_service',
					name='cf-all64-ubuntu1004',
					hosts=[queryenv.RoleHost(index='1', replication_master='1',
					internal_ip="10.242.75.80", 	 external_ip='50.17.99.58'),
					queryenv.RoleHost(index='1', replication_master='1',
					internal_ip="10.242.75.80", 	 external_ip='50.17.99.58')]),
				]
		self.output(res)

class LRPC(szradm.ListRoleParamsCommand):
	def run(self):
		self.output({'key1':'val1','key2':'val2','key3':'val3','key4':'val4'})

class LVC(szradm.ListVirtualhostsCommand):
	def run(self):
		res=[queryenv.VirtualHost(hostname='194.162.85.4', type='virHost', raw='<![CDATA[ ]]>', https=False),
			queryenv.VirtualHost(hostname='201.1.85.4', type='virHost2', raw='<![CDATA[ ]]>', https=True)]
		self.output(res)

class LSC(szradm.ListScriptsCommand):
	def run(self):
		self.output([queryenv.Script(asynchronous=False, exec_timeout=126, name='Script1',
			body='<script> ... </script>'), queryenv.Script(asynchronous=True,
			exec_timeout=12006, name='Script2', body='<script> ... </script>')])

class Test(unittest.TestCase):
	def test_ListEbsMountpointsCommand(self):
		LEM().run()
	
	def test_ListRolesCommand(self):
		LRC().run
	
	def test_ListRoleParamsCommand(self):
		LRPC().run()
		
	def test_ListVirtualhostsCommand(self):
		LVC().run()
		
	def test_ListScriptsCommand(self):
		LSC().run()
	
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()


'''
* szadm --queryenv list-roles behaviour=app
* szadm --msgsnd -n BlockDeviceAttached devname=/dev/sdo
* szadm --msgsnd --lo -n IntServerReboot
* szadm --msgsnd --lo -f rebundle.xml
* szadm --repair host-up
'''
"""
<?xml version="1.0" ?>
<message id="037b1864-4539-4201-ac0b-5b1609686c80" name="Rebundle">
    <meta>
        <server_id>ab4d8acc-f001-4666-8f87-0748af52f700</server_id>
    </meta>
    <body>
        <platform_access_data>
            <account_id>*account_id*</account_id>
            <key_id>*key_id*</key_id>
            <key>*key*</key>
            <cert>*cert*</cert>
            <pk>*pk*</pk>
        </platform_access_data>
        <role_name>euca-base-1</role_name>
        <bundle_task_id>567</bundle_task_id>
        <excludes><excludes>
    </body>
</message>
"""	
	