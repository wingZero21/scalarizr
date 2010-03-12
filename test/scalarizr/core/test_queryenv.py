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
		self._queryenv = QueryEnvService("http://ec2farm-dev.bsd2.webta.local/query-env/","127", "i-c9acf6a1", \
										 "c+/g0PyouaqXMbuJ5Vtux34Mft7jLe5H5u8tUmyhldjwTfgm7BI6MOA8F6BwkzQnpWEOcHx+A+TRJh0u3PElQQ0SiwdwrlgpQMbj8NBxbxBgfxA9WisgvfQu5ZPYou6Gz3oUAQdWfFlFdY2ACOjmqa3DGogge+TlXtV2Xagm0rw=",\
										 "5d0e16f7498c41cc")
		
	def _test_get_latest_version_response(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/get_latest_version_response.xml"		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		
		version = self._queryenv._read_get_latest_version_response(xml)
		
		self.assertTrue(not version is None)
		self.assertEqual(version, "2009-03-05")
	
	def _test_get_https_certificate_response(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/get_https_certificate_response.xml"
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		cert = self._queryenv._read_get_https_certificate_response(xml)
		self.assertTrue(not cert is None)
		#self.assertEqual(cert[0], "MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN")
		#self.assertEqual(cert[1], "MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhO")


	def _test_list_roles(self):
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
		self.assertEqual(host.index, 1)
		
	
	def _test_read_list_ebs_mountpoints_response(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/list_ebs_mountpoints_response.xml"
		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		 
		mountpoints = self._queryenv._read_list_ebs_mountpoints_response(xml)
		
		self.assertTrue(not mountpoints is None)
		self.assertEqual(len(mountpoints), 2)
		mountpoint = mountpoints[0]
		self.assertEqual(mountpoint.name, "some_name_for_LVM")
		self.assertEqual(mountpoint.dir, "/mnt/storage1")
		self.assertTrue(mountpoint.create_fs)
		self.assertFalse(mountpoint.is_array)
		
		volumes = mountpoint.volumes
		self.assertTrue(volumes is not None)
		volume = volumes[0]
		self.assertEqual(volume.volume_id, "vol-123451")
		self.assertEqual(volume.device, "/dev/sdb")
		
	
		
	def _test_list_role_params(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/list_role_params_response.xml"
		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		 
		parametres = self._queryenv._read_list_role_params_response(xml)
		
		self.assertTrue(not parametres is None)
		self.assertTrue(parametres.has_key("external_ips_to_allow_access_from"))
		self.assertEqual(parametres.get("external_ips", None), None)
		self.assertEqual(parametres.get("external_ips_to_allow_access_from", None), """

                                """)
		
	def _test_read_list_scripts_response(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/list_scripts_response.xml"
		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		
		scripts = self._queryenv._read_list_scripts_response(xml)
		
		self.assertTrue(not scripts is None)
		self.assertEqual(len(scripts), 1)
		script = scripts[0]
		self.assertTrue(script.asynchronous)
		self.assertEqual(script.exec_timeout, 100)
		self.assertEqual(script.name, 'script_name')
		self.assertEqual(script.body, """
                
                                """)
		
	def _test_read_list_virtualhosts_response(self):
		xmlfile = os.path.dirname(__file__) + "/../../resources/list_virtualhosts_response.xml"
		
		from xml.dom.minidom import parseString
		xml = parseString(open(xmlfile, "r").read())
		
		vhosts = self._queryenv._read_list_virtualhosts_response(xml)
		
		self.assertTrue(not vhosts is None)
		self.assertEqual(len(vhosts), 2)
		vhost = vhosts[0]
		self.assertEqual(vhost.hostname, 'gpanel.net')
		self.assertEqual(vhost.type, 'apache')
		self.assertEqual(vhost.raw, '''
                                
                                ''')
		self.assertTrue(vhosts[1].https)
	
	def test_sign(self):
		str = "I Can Has Cheezburger?"
		key = "somesecurekeystring"
		sign = self._queryenv._sign(str, key)
		self.assertTrue(sign)
		
	def test__get_http_timestamp(self):	
		time = self._queryenv._get_http_timestamp()
		#print time
	
	def test_get_latest_version(self):	
		self.setUp()
		version = self._queryenv.get_latest_version()
		#print "version>> ", version
		self.assertEquals(version, '2009-03-05')
	
	def test_get_https_certificate(self):
		cert = self._queryenv.get_https_certificate()
		#print "cert >> ", cert
		
	def test_list_ebs_mountpoints(self):
		ebs_list = self._queryenv.list_ebs_mountpoints ()
		#print "ebs_list>> ", ebs_list
		
	def test_list_role_params(self):
		list_params = self._queryenv.list_role_params("www")
		#print "list_role_params>> ", list_params
		
	def test_list_roles(self):
		list_roles = self._queryenv.list_roles("www")
		#print "list_roles>> ", list_roles
		
	def test_list_scripts(self):
		list_scripts = self._queryenv.list_scripts()
		#print "list_scripts>> ", list_scripts
		
	def test_list_virtualhosts(self):
		list_virtualhosts = self._queryenv.list_virtual_hosts("www")
		#print "list_virtualhosts>> ", list_virtualhosts
		
		#FarmID = 127
		#InstanceID = i-c9acf6a1
		#Instance Public IP = 75.101.190.84
		#Instance Private IP = 10.245.205.207
		#Query env URL: http://ec2farm-dev.bsd2.webta.local/environment.php
		#Latest version: 2009-03-05
		#Key ID = 5d0e16f7498c41cc
		# Key = c+/g0PyouaqXMbuJ5Vtux34Mft7jLe5H5u8tUmyhldjwTfgm7BI6MOA8F6BwkzQnpWEOcHx+A+TRJh0u3PElQQ0SiwdwrlgpQMbj8NBxbxBgfxA9WisgvfQu5ZPYou6Gz3oUAQdWfFlFdY2ACOjmqa3DGogge+TlXtV2Xagm0rw=
	
	def test_get_canonical_string(self):
	   dict = {2:"two",3:"three",1:"one",4:"four"}
	   str = self._queryenv._get_canonical_string(dict)
	   self.assertEqual(str,"1one2two3three4four")

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_list_roles']
	unittest.main()