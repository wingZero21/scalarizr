'''
Created on Feb 14th, 2011

@author: Dmytro Korsakov
'''

class MachineImage(object):
	'''
	classdocs
	'''
	name = None
	file = None
	attributes = None
	account = None
	
	
	def __init__(self, name=None, file=None, attributes=None, account = None):
		self.name = name
		self.file = file
		self.attributes = attributes or {}
		self.account = account
		