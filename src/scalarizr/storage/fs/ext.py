'''
Created on Nov 11, 2010

@author: marat
'''
from . import FileSystem

class ExtFileSystem(FileSystem):
	pass

filesystems = dict(ExtFileSystem=('ext2', 'ext3', 'ext4'))

