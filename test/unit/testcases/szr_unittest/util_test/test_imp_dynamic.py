'''
Created on 22.03.2012

@author: sam
'''
import unittest
import os, sys, logging
import ConfigParser as configparser

from scalarizr.util import disttool
from scalarizr.util import system2
import mock

from scalarizr.util import imp_dynamic

LOG = logging.getLogger(__name__)

class TestImpLoader2(unittest.TestCase):
	def setUp(self):
		disttool = mock.Mock()

	def test_redhat_install_pypackage(self):
		'''test_redhat_install_pypackage'''
		disttool.linux_dist = mock.MagicMock(return_value =('redhat', '5.6', 'final'))
		disttool.is_redhat_based = mock.MagicMock(return_value =True)
		disttool.is_debian_based = mock.MagicMock(return_value =False)

		self.imp = imp_dynamic.ImpImport(manifest_path='find-pypackage_name-in-manifest.ini')
		self.imp.mgr.install = mock.Mock()
		self.imp.mgr.candidates = mock.MagicMock(return_value = ['2.0', '3.10'])
		
		self.imp.install_pypackage('PyYAML')
		self.imp.mgr.install.assert_called_with('python26-pyyaml', '3.10')
		
	
	def test_ubuntu_install_pypackage(self):
		'''test_ubuntu_install_pypackage'''
		disttool.is_redhat_based = mock.MagicMock(return_value =False)
		disttool.is_debian_based = mock.MagicMock(return_value =True)
		disttool.linux_dist = mock.MagicMock(return_value =('Ubuntu', '11.4', 'oneiric'))
		
		self.imp = imp_dynamic.ImpImport(manifest_path='find-pypackage_name-in-manifest.ini')
		self.imp.mgr.install = mock.Mock()
		self.imp.mgr.candidates = mock.MagicMock(return_value = ['2.0', '3.10'])
		self.imp.install_pypackage('PyYAML')
		self.imp.mgr.install.assert_called_with('python-pyyaml', '3.10')


	def test_install_pypackage_if_didnt_found_package_versions(self):
		'''test_didnt_found_package_versions_in_install_pypackage'''

		disttool.is_redhat_based = mock.MagicMock(return_value =True)
		disttool.is_debian_based = mock.MagicMock(return_value =False)
		disttool.linux_dist = mock.MagicMock(return_value =('redhat', '5.6', 'final'))
		self.imp = imp_dynamic.ImpImport(manifest_path='find-pypackage_name-in-manifest.ini')
		self.imp.mgr.candidates = mock.MagicMock(return_value = [])
		#self.imp.mgr.install = mock.MagicMock(return_value = Exception('error install package'))
		try:
			self.imp.install_pypackage('Freak')
			self.fail('not raising exception, when it must')
		except Exception, e:
			self.assertRaises(Exception, e)

"""
class TestImpLoader(unittest.TestCase):

	def setUp(self):
		unittest.TestCase.setUp(self)

		with open(PATH2IMANIFEST, 'w+') as fp:
			fp.write('')
		conf = configparser.ConfigParser()
		manif = {'apt':{'snmp' : 'python-pysnmp4',
						'pymongo' : 'python-pymongo',
						'prettytable' : 'python-prettytable',
						'pyyaml' : 'python-PyYAML',
						'm2crypto' : 'python-m2crypto'},
				'yum':{'snmp' : 'python26-pysnmp4',
						'pymongo' : 'python26-pymongo',
						'prettytable' : 'python26-prettytable',
						'pyyaml' : 'python26-PyYAML',
						'M2Crypto' : 'python26-m2crypto'},
				'apt:ubuntu804':{},
				'apt:ubuntu1004':{},
				'yum:el5':{},
				'yum:centos5':{},
				'yum:el6':{}}

		for section in manif.keys():
			conf.add_section(section)
			for option in manif[section].keys():
				conf.set(section, option, manif[section][option])

		with open(PATH2IMANIFEST, 'w+') as fp:
			conf.write(fp)

		imp_dynamic.setup(path=PATH2IMANIFEST)
		if disttool.is_debian_based():
			system2(('apt-get remove -y python-prettytable & '\
					'apt-get remove -y python-PyYAML & '\
					'apt-get remove -y python-m2crypto & '\
					'apt-get remove -y python-pymongo'), )

		elif disttool.is_redhat_based():
			'''
			system2(('yum remove -y python-prettytable & '\
					'yum remove -y python-PyYAML & '\
					'yum remove -y python-m2crypto & '\
					'yum remove -y python-pymongo'), )'''
		#apt-get remove -y -q -m python-prettytable & apt-get remove -q -m -y python-PyYAML & apt-get remove -y -q -m python-m2crypto & apt-get remove -q -m -y python-pymongo
		
	def test_apt(self):
		if disttool.is_debian_based():
			import pymongo
			LOG.debug('Pymongo version: %s', pymongo.version.title())
			try:
				import prettytable
			except:
				pass
		else:
			self.fail('can`t test `apt` on RedHat-based OS')

	def test_apt_m2crypto(self):
		if disttool.is_debian_based():
			try:
				import M2Crypto
			except:
				self.fail('m2crypto not install')
		else:
			self.fail('can`t test `apt` on RedHat-based OS')

	def test_yum_prettytable(self):
		if disttool.is_redhat_based():		
			import prettytable
			
			LOG.debug('Pymongo version: %s', pymongo.version.title())
		else:
			self.fail('can`t test `yum` on debian-based OS')
			
	def test_yum_pymongo(self):
		if disttool.is_redhat_based():		
			import pymongo
			LOG.debug('Pymongo version: %s', pymongo.version.title())
		else:
			self.fail('can`t test `yum` on debian-based OS')
			

	def test_yum_M2Crypto(self):
		if disttool.is_redhat_based():
			import M2Crypto
			#self.assertRaises(ImportError, sys.exc_info())
		else:
			self.fail('can`t test `yum` on debian-based OS')
"""

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_apt_manager']
	unittest.main()