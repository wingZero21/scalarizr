'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr.util import initdv2
from scalarizr.util import disttool as dist
from scalarizr.util import filetool
from scalarizr.util import which


class HAProxyCfg(filetool.ConfigurationFile):
	DEFAULT = '/etc/haproxy/haproxy.cfg'
	
	def __init__(self, path=None):
		super(HAProxyCfg, self).__init__(path or self.DEFAULT)
	
	def backends(self):
		pass
	
	def listeners(self):
		pass

	
class HAProxyInitScript(initdv2.ParametrizedInitScript):
	
	def __init__(self):
		if dist.redhat or dist.ubuntu >= (10, 4):
			init_script = (which('service'), 'haproxy')
		else:
			init_script = '/etc/init.d/haproxy'
			
		super(HAProxyInitScript, self).__init__('haproxy', init_script)