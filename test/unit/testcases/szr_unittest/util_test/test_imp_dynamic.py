'''
Created on 22.03.2012

@author: sam
'''
import unittest
import os, sys, logging

from scalarizr.util import imp_dynamic

from scalarizr.util import disttool
from scalarizr.util import system2
try:
	import ConfigParser as configparser
except:
	import configparser as configparser

PATH2IMANIFEST = '/tmp/manifest.ini'

LOG = logging.getLogger(__name__)

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
						'm2crypto' : 'python26-m2crypto'},
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

		imp_dynamic.setup(path = PATH2IMANIFEST)
		"""
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
		"""
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

	def test_apt_broken(self):
		if disttool.is_debian_based():
			try:
				import m2crypto
			except:
				LOG.debug(exc_info=sys.exc_info())
		else:
			self.fail('can`t test `apt` on RedHat-based OS')

	def test_yum(self):
		if disttool.is_redhat_based():		
			import pymongo
			LOG.debug('Pymongo version: %s', pymongo.version.title())
		else:
			self.fail('can`t test `yum` on debian-based OS')

	def test_yum_broken(self):
		if disttool.is_redhat_based():
			try:
				import m2crypto
			except:
				self.assertRaises(ImportError, sys.exc_info())
		else:
			self.fail('can`t test `yum` on debian-based OS')

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_apt_manager']
	unittest.main()

'''
#test manifest look like this:
[apt]
pysnmp = python-pysnmp4
pymongo = python-pymongo
prettytable = python-prettytable
pyyaml = python-PyYAML
m2crypto = python-m2crypto

[yum]
pysnmp = python26-pysnmp
pyyaml = python26-PyYAML
m2crypto = python26-m2crypto
pexpect = python26-pexpect
pysnmp = python26-pysnmp
pysnmp-mibs = python26-pysnmp-mibs
prettytable = python26-prettytable

[apt=ubuntu804]
zmq = python25-zeromq

[yum=el5]
pymongo = python26-pymongo

[yum=centos5]
pymongo = python26-pymongo

[yum=el6]
pymongo = python-pymongo
'''