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
    _platform = None
    _linux_version = None
    _is_debian_based = False
    _is_redhat_based = False
    _debian_based_dists = ['debian', 'Ubuntu']
    _rpm_based_dists = ['CentOS', 'RHEL']
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            
            cls._instance._platform = cls._instance._get_platform()
            if 'Linux' == cls._instance._platform:
                cls._instance._linux_version = cls._instance._get_linux_version()
                cls._instance._get_linux_base()
                
            return cls._instance
    
    def _get_platform(self):
        return platform.uname()[0]
    
    def _get_linux_version(self):
        return platform.linux_distribution()[0]
    
    def _get_linux_base(self):
        if self._linux_version in self._debian_based_dists:
            self._is_debian_based = True
        if self._linux_version in self._rpm_based_dists:
            self._is_redhat_based = True                
            
    def is_debian_based(self):
        return self._is_debian_based
    
    def is_redhat_based(self):
        return self._is_redhat_based
    
    def is_linux(self):
        return 'Linux' == self._platform
    