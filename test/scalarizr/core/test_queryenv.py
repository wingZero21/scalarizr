'''
Created on Dec 23, 2009

@author: marat
'''
import unittest


class Test(unittest.TestCase):

	_queryenv

	def test_list_roles(self):
		from xml.dom.minidom import getDOMImplementation
		
		 
		roles = self._queryenv._read_list_roles_response(xml)
		
		self.assertEqual(len(roles), 3)
		
		pass


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_list_roles']
	unittest.main()