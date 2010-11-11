'''
Created on Nov 11, 2010

@author: marat
'''


from . import FileSystem

class JfsFileSystem(FileSystem):
	pass

filesystems = dict(JfsFileSystem=('jfs', 'jfs2'))
