'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
import unittest
import os

class Test(unittest.TestCase):

	_queryenv = None		

	def setUp (self):
		from scalarizr.core.queryenv import QueryEnvService
		self._queryenv = QueryEnvService("", None, None)

	def test_list_roles(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/list_roles_response.xml"
		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		 
		roles = self._queryenv._read_list_roles_response(xml)
		
		self.assertTrue(not roles is None)
		self.assertEqual(len(roles), 3)
		role = roles[0]
		self.assertEqual(role.behaviour, "mysql")
		self.assertEqual(role.name, "mysql-lvm")
		hosts = role.hosts
		self.assertTrue(hosts is not None)
		host = hosts[0]
		self.assertEqual(host.internal_ip, "211.31.14.198")
		self.assertEqual(host.external_ip, "211.31.14.198")
		self.assertTrue(host.replication_master)
		
	def test_list_role_params(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/list_role_params_response.xml"
		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		 
		parametres = self._queryenv._read_list_role_params_response(xml)
		
		self.assertTrue(not parametres is None)
		self.assertTrue(parametres.has_key("external_ips_to_allow_access_from"))
		self.assertEqual(parametres.get("external_ips", None), None)
		self.assertEqual(parametres.get("external_ips_to_allow_access_from", None), """

                                """)


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_list_roles']
	unittest.main()