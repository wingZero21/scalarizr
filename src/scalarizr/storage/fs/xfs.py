'''
Created on Nov 11, 2010

@author: marat
'''

from . import FileSystem

class XfsFileSystem(FileSystem):
	pass

filesystems = dict(XfsFileSystem=('xfs',))
