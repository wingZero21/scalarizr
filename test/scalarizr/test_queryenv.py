'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
import os
import unittest
from xml.dom.minidom import parseString
from scalarizr.queryenv import QueryEnvService

class Test(unittest.TestCase):		

	def setUp (self):
		self._queryenv = QueryEnvService("http://ec2farm-dev.bsd2.webta.local/query-env/","127","c+/g0PyouaqXMbuJ5Vtux34Mft7jLe5H5u8tUmyhldjwTfgm7BI6MOA8F6BwkzQnpWEOcHx+A+TRJh0u3PElQQ0SiwdwrlgpQMbj8NBxbxBgfxA9WisgvfQu5ZPYou6Gz3oUAQdWfFlFdY2ACOjmqa3DGogge+TlXtV2Xagm0rw=")
		
	def test_get_latest_version_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/get_latest_version_response.xml"		
		xml = parseString(open(xmlfile, "r").read())		
		version = self._queryenv._read_get_latest_version_response(xml)
		self.assertFalse(version is None)
		self.assertEqual(version, "2009-03-05")
	
	def test_get_https_certificate_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/get_https_certificate_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		cert = self._queryenv._read_get_https_certificate_response(xml)
		self.assertFalse(cert is None)
		self.assertEqual(cert[0], "MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN")
		self.assertEqual(cert[1], "MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhO")

	def test_list_roles(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_roles_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		roles = self._queryenv._read_list_roles_response(xml)
		role = roles[0]
		hosts = role.hosts
		host = hosts[0]
		self.assertFalse(roles is None)
		self.assertEqual(len(roles), 3)
		self.assertEqual(role.behaviour, "mysql")
		self.assertEqual(role.name, "mysql-lvm")
		self.assertFalse(hosts is None)
		self.assertEqual(host.internal_ip, "211.31.14.198")
		self.assertEqual(host.external_ip, "211.31.14.198")
		self.assertTrue(host.replication_master)
		self.assertEqual(host.index, 1)
	
	def test_read_list_ebs_mountpoints_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_ebs_mountpoints_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		mountpoints = self._queryenv._read_list_ebs_mountpoints_response(xml)
		mountpoint = mountpoints[0]
		volumes = mountpoint.volumes
		volume = volumes[0]
		self.assertFalse(mountpoints is None)
		self.assertEqual(len(mountpoints), 2)
		self.assertEqual(mountpoint.name, "some_name_for_LVM")
		self.assertEqual(mountpoint.dir, "/mnt/storage1")
		self.assertTrue(mountpoint.create_fs)
		self.assertFalse(mountpoint.is_array)
		self.assertFalse(volumes is None)
		self.assertEqual(volume.volume_id, "vol-123451")
		self.assertEqual(volume.device, "/dev/sdb")
		
	def test_list_role_params(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_role_params_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		parametres = self._queryenv._read_list_role_params_response(xml)
		self.assertFalse(parametres is None)
		self.assertTrue(parametres.has_key("external_ips_to_allow_access_from"))
		self.assertEqual(parametres.get("external_ips_to_allow_access_from", None), """

                                """)
		
	def test_read_list_scripts_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_scripts_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		scripts = self._queryenv._read_list_scripts_response(xml)
		script = scripts[0]
		self.assertFalse(scripts is None)
		self.assertEqual(len(scripts), 1)
		self.assertTrue(script.asynchronous)
		self.assertEqual(script.exec_timeout, 100)
		self.assertEqual(script.name, 'script_name')
		self.assertEqual(script.body, """
                
                                """)

	def test_read_list_virtualhosts_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_virtualhosts_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		vhosts = self._queryenv._read_list_virtualhosts_response(xml)
		vhost = vhosts[0]
		self.assertFalse(vhosts is None)
		self.assertEqual(len(vhosts), 2)
		self.assertEqual(vhost.hostname, 'gpanel.net')
		self.assertEqual(vhost.type, 'apache')
		self.assertEqual(vhost.raw, '''
                                
                                ''')
		self.assertTrue(vhosts[1].https)

	def test_sign(self):
		str = "Can I Has Cheezburger?"
		key = "securekeystring"
		sign = self._queryenv._sign(str, key)
		self.assertEqual(sign,"fCNPytSqqOy8QTI5L+nZ9AzRMzs=")
		
	def test_get_canonical_string(self):
		dict = {2:"two",3:"three",1:"one",4:"four"}
		str = self._queryenv._get_canonical_string(dict)
		self.assertEqual(str,"1one2two3three4four")
	
#	def test_get_latest_version(self):	
		#self.setUp()
#		version = self._queryenv.get_latest_version()
#		self.assertEquals(version, '2009-03-05')

if __name__ == "__main__":
	unittest.main()		
#FarmID = 127
#InstanceID = i-c9acf6a1
#Instance Public IP = 75.101.190.84
#Instance Private IP = 10.245.205.207
#Query env URL: http://ec2farm-dev.bsd2.webta.local/environment.php
#Latest version: 2009-03-05
#Key ID = 5d0e16f7498c41cc
# Key = c+/g0PyouaqXMbuJ5Vtux34Mft7jLe5H5u8tUmyhldjwTfgm7BI6MOA8F6BwkzQnpWEOcHx+A+TRJh0u3PElQQ0SiwdwrlgpQMbj8NBxbxBgfxA9WisgvfQu5ZPYou6Gz3oUAQdWfFlFdY2ACOjmqa3DGogge+TlXtV2Xagm0rw=
