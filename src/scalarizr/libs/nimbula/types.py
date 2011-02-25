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
	
	
	def __init__(self, name=None, file=None, attributes=None, account = None, from_response=None):
		if from_response:
			self._deserialize(from_response)
		if name:
			self.name = name
		if file:
			self.file = file
		if attributes:
			self.attributes = attributes or {}
		if account:
			self.account = account
		
	def _deserialize(self, entry):							
		self.name = entry['name']
		self.file = entry['file']
		self.attributes = entry['attributes']
		self.account = entry['account']
		
	def __eq__(self, other):
		return self.name == other.name
		
	def __repr__(self):
		return 'name: %s, file: %s, attrs: %s, account %s' % \
				(self.name, self.file, self.attributes, self.account)
				
