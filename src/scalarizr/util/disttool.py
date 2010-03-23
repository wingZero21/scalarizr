# -*- coding: latin-1 -*-
'''
Created on 23 марта 2010

@author: Dmytro Korsakov
'''
import platform

class DistTool(object):
	'''
	classdocs
	'''
	_instance = None
	
	_uname = None
	_linux_dist = None
	
	_is_linux = _is_win = _is_sun = False
	_is_debian_based = _is_redhat_based = False
	_is_debian = _is_ubuntu = False
	_is_rhel = _is_centos = _is_fedora = False
	
	_debian_based_dists = ['debian', 'ubuntu']
	_redhat_based_dists = ['centos', 'rhel', 'redhat', 'fedora']

	def __new__(cls):
		if cls._instance is None:
			i = object.__new__(cls)
			cls._instance = i			

			i._uname = platform.uname()
			os = i._uname[0].lower()
			i._is_linux = os == "linux"
			i._is_win = os == "windows"
			i._is_sun = os == "sunos"
			
			if i._is_linux:
				i._linux_dist = platform.linux_distribution() \
					if hasattr(platform, "linux_distribution") \
					else platform.dist()
				dist_name = i._linux_dist[0].lower()
				i._is_redhat_based = dist_name in i._redhat_based_dists
				i._is_debian_based = dist_name in i._debian_based_dists
				i._is_debian = dist_name == "debian"
				i._is_ubuntu = dist_name == "ubuntu"
				i._is_fedora = dist_name == "fedora"
				i._is_centos = dist_name == "centos"
				i._is_rhel = dist_name in ["rhel", "redhat"]
			
		return cls._instance

	def is_linux(self): return self._is_linux
	def is_win(self): return self._is_win
	def is_sun(self): return self._is_sun
	
	def is_debian_based(self): return self._is_debian_based
	def is_redhat_based(self): return self._is_redhat_based
	
	def is_fedora(self): return self._is_fedora
	def is_centos(self): return self._is_centos
	def is_rhel(self): return self._is_rhel
	def is_ubuntu(self): return self._is_ubuntu
	def is_debian(self): return self._is_debian

	def uname(self): return self._uname
	def linux_dist(self): return self._linux_dist
		
