# -*- coding: latin-1 -*-
'''
Created on 23 марта 2010

@author: shaitanich
'''
import platform
import os

class DistTool(object):
    '''
    classdocs
    '''
    obj = None
    _version = None
    _dist = None
    _deb = ['debian', 'Ubuntu']
    _rpm = ['CentOS', 'RHEL']
    
    def __new__(cls,*dt,**mp):
        if cls.obj is None:
            cls.obj = object.__new__(cls,*dt,**mp)
            cls.obj._version = cls.obj.get_version()
            if os.path.exists("/etc/debian_version") or cls.obj._version in cls.obj._deb:
                cls.obj._dist = "deb"
            elif os.path.exists("/etc/redhat-release") or cls.obj._version in cls.obj._rpm:
                cls.obj._dist = "rpm"    
            return cls.obj
    
    def is_debian_based(self):
        if self._dist == "deb":
            return True
        else:
            return False

    def is_redhat_based(self):
        if self._dist == "rpm":
            return True
        else:
            return False
   
    def get_version(self):
        return platform.linux_distribution()[0]
        

