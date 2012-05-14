'''
Created on 22.03.2012

@author: sam
'''

import os
import sys

from scalarizr.util import disttool, dynimp

import mock
from nose.tools import raises


class TestImpLoader(object):
	def setup(self):
		self.manifest = os.path.dirname(__file__) + '/dynimp-manifest.ini'
		
		disttool.linux_dist = mock.Mock(return_value = ('', '', ''))
		disttool.is_debian_based = mock.Mock(return_value=False)		
		disttool.is_redhat_based = mock.Mock(return_value=False)
		disttool.is_fedora = mock.Mock(return_value=False)


	def test_redhat_install_python_package(self):
		disttool.linux_dist.return_value = ('redhat', '5.6', 'final')
		disttool.is_redhat_based.return_value = True
		disttool.is_debian_based.return_value = False

		self.imp = dynimp.ImpLoader(self.manifest)
		self.imp.mgr.install = mock.Mock()		
		self.imp.mgr.candidates = mock.Mock(return_value = ['2.0', '3.10'])
		
		self.imp.install_python_package('PyYAML')
		self.imp.mgr.install.assert_called_with('python26-pyyaml', '3.10')
		
	
	def test_ubuntu_install_python_package(self):
		disttool.linux_dist.return_value = ('Ubuntu', '12.04', 'oneiric')		
		disttool.is_redhat_based.return_value = False
		disttool.is_debian_based.return_value = True

		self.imp = dynimp.ImpLoader(self.manifest)
		self.imp.mgr.install = mock.Mock()		
		self.imp.mgr.candidates = mock.Mock(return_value = ['2.0', '3.10'])
		
		self.imp.install_python_package('PyYAML')
		self.imp.mgr.install.assert_called_with('python-pyyaml', '3.10')

		
	@raises(ImportError)
	def test_install_python_package_os_package_has_no_candidates(self):
		disttool.linux_dist.return_value = ('redhat', '5.6', 'final')
		disttool.is_redhat_based.return_value = True
		disttool.is_debian_based.return_value = False

		self.imp = dynimp.ImpLoader(self.manifest)
		self.imp.mgr.install = mock.Mock()		
		self.imp.mgr.candidates = mock.Mock(return_value = [])
		
		self.imp.install_python_package('Freak')
		assert False, 'ImportError expected but never raised'
		
	@raises(ImportError)		
	def test_install_python_package_no_os_package_mapping(self):
		disttool.linux_dist.return_value = ('redhat', '5.6', 'final')
		disttool.is_redhat_based.return_value = True
		disttool.is_debian_based.return_value = False

		self.imp = dynimp.ImpLoader(self.manifest)
		self.imp.mgr.install = mock.Mock()		
		
		self.imp.install_python_package('unknown-package')
		assert False, 'ImportError expected but never raised'

	'''
	def test_import(self):
		disttool.linux_dist.return_value = ('Ubuntu', '12.04', 'oneiric')		
		disttool.is_redhat_based.return_value = False
		disttool.is_debian_based.return_value = True

		self.imp = dynimp.ImpLoader(self.manifest)
		self.imp.mgr.install = mock.Mock()		
		self.imp.mgr.candidates = mock.Mock(return_value = ['1.01a-1ubuntu0'])
		
		sys.meta_path += [self.imp]
		__import__('package_to_install')
		
		self.imp.mgr.install.assert_called_with('python-package-to-install', '1.01a-1ubuntu0')
	'''