'''
Created on Aug 29, 2012

@author: marat
'''

from scalarizr import storage2
from scalarizr.storage2.filesystems import ext3  


class Ext4FileSystem(ext3.ExtFileSystem):
	name = 'ext4'


storage2.filesystem_types['ext4'] = Ext4FileSystem
