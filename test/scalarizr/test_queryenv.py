'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
import os
import unittest
from xml.dom.minidom import parseString
from scalarizr.queryenv import QueryEnvService
from scalarizr.util import xml_strip

class Test(unittest.TestCase):		

	def setUp (self):
		self._queryenv = QueryEnvService("http://ec2farm-dev.bsd2.webta.local/query-env/","127","c+/g0PyouaqXMbuJ5Vtux34Mft7jLe5H5u8tUmyhldjwTfgm7BI6MOA8F6BwkzQnpWEOcHx+A+TRJh0u3PElQQ0SiwdwrlgpQMbj8NBxbxBgfxA9WisgvfQu5ZPYou6Gz3oUAQdWfFlFdY2ACOjmqa3DGogge+TlXtV2Xagm0rw=")
		
	def test_get_latest_version_response(self):
		xmlfile = os.path.realpath(os.path.dirname(__file__) + "/../resources/get_latest_version_response.xml")
		xml = xml_strip(parseString(open(xmlfile, "r").read()))
		version = self._queryenv._read_get_latest_version_response(xml)
		self.assertFalse(version is None)
		self.assertEqual(version, "2009-03-05")
	
	def test_get_https_certificate_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/get_https_certificate_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		xml = xml_strip(xml)
		cert = self._queryenv._read_get_https_certificate_response(xml)
		self.assertFalse(cert is (None,None))
		self.assertEqual(cert[0], """-----BEGIN CERTIFICATE-----
MIICATCCAWoCCQDVWoPxl2kzdzANBgkqhkiG9w0BAQUFADBFMQswCQYDVQQGEwJB
VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0
cyBQdHkgTHRkMB4XDTEwMDYwNDEyMDcyNloXDTExMDYwNDEyMDcyNlowRTELMAkG
A1UEBhMCQVUxEzARBgNVBAgTClNvbWUtU3RhdGUxITAfBgNVBAoTGEludGVybmV0
IFdpZGdpdHMgUHR5IEx0ZDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA5DeJ
DnVd/Jgb3bPh296l78V/T7kRG8IAejawlR25ikUXpG9DoaV05nDwDR+e3Hm/wvmY
7EwqOiVFtFc1Uoc6av34zIroEcGmwi+nnUAptpHOC/863VhuSehKf7lWkUbLz8OS
NAJOj5c6jxKfasnkqxYEakhvXCe/N4NQWdnSaVUCAwEAATANBgkqhkiG9w0BAQUF
AAOBgQAJ318EcccEM34pFsF5thGQfYU4yrTb2P+Xyg/bhNgByCc+agWh9MAXBF1G
EEH5rvsHc1ocVXi69a45D+m0pV5ZJwSIrwo6ssViMpWmUfIStkmYm7qsRbNnIWkZ
TuMUjy2djQJdeAadKNinJ5YXk2iU7XvxTzaqZAzdpv2/9G5Nbg==
-----END CERTIFICATE-----
""")
		self.assertEqual(cert[1], """-----BEGIN RSA PRIVATE KEY-----
MIICXgIBAAKBgQDkN4kOdV38mBvds+Hb3qXvxX9PuREbwgB6NrCVHbmKRRekb0Oh
pXTmcPANH57ceb/C+ZjsTCo6JUW0VzVShzpq/fjMiugRwabCL6edQCm2kc4L/zrd
WG5J6Ep/uVaRRsvPw5I0Ak6PlzqPEp9qyeSrFgRqSG9cJ783g1BZ2dJpVQIDAQAB
AoGBAKNtDY24CIUBHDtoPF4qE6QfRur9OT4qcBMUpOEztxFIJwUO1FymUo9O4uhS
830pBmSGPrdAV6Dp3f+lz754riBj1Hzk5kQuF6rAxoBspbqXQB30Pz1r6qWt9Sf2
DZqQ9278UZEtUQq90QzEjh3xAV5BxG1Qv+d3yyIVy1K5Pt7BAkEA957XKSdGpQW5
c2V6rUnfvLi/WeIx8xFFL1ohnPSBLhvP8Hc4zJGVVBnuX9RKEVVLCizRBLFK93uI
nh0aGSa9hQJBAOvwmamWMkFH4aYymqZY5CRooxCG6Wv8MTQBhgGaLyGYX/CaHpRf
Y5RCEWnXZJr/rAwnowLr8kh/MiZGIBWVHZECQQDBeSRYDU4PRke+OD4AA8aC6D7q
defdKVNLSjsVLZ15b1WrZxvECsQIcDJmQbKVlHULQDUYW4Zdk/IMyGRJ3pEZAkEA
jWaz4RQX6FHZJY7cameJy1w+phAE4ufQ4TcshddO+dZlYUAspYWJm3gBEaq6K76g
8OPsaTrZCKPafV+3qNemUQJACZ7FDJKmO9SLccpYIDTcMIhqgu2QseZwjJPjAMbg
0xHR3hyeMnkrjP9amXrbAxOwndCD10I6Tuw4Qj7t20p+hw==
-----END RSA PRIVATE KEY-----
""")

	def test_list_roles(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_roles_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		xml = xml_strip(xml)
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
		xml = xml_strip(xml)
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
		xml = xml_strip(xml)
		parametres = self._queryenv._read_list_role_params_response(xml)
		self.assertFalse(parametres is None)
		#self.assertTrue(parametres.has_key("external_ips_to_allow_access_from"))
		self.assertEqual(parametres.get("external_ips_to_allow_access_from", None), """

                                """)
		
	def test_read_list_scripts_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_scripts_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		xml = xml_strip(xml)
		scripts = self._queryenv._read_list_scripts_response(xml)
		script = scripts[0]
		self.assertFalse(scripts is None)
		self.assertEqual(len(scripts), 1)
		self.assertTrue(script.asynchronous)
		self.assertEqual(script.exec_timeout, 100)
		self.assertEqual(script.name, 'script_name')
		self.assertEqual(script.body, """
                
                                """)

	def _test_read_list_virtualhosts_response(self):
		xmlfile = os.path.dirname(__file__) + "/../resources/list_virtualhosts_response.xml"
		xml = parseString(open(xmlfile, "r").read())
		xml = xml_strip(xml)
		vhosts = self._queryenv._read_list_virtualhosts_response(xml)
		vhost = vhosts[0]
		self.assertFalse(vhosts is None)
		self.assertEqual(len(vhosts), 2)
		self.assertEqual(vhost.hostname, 'gpanel.net')
		self.assertEqual(vhost.type, 'apache')
		self.assertEqual(vhost.raw, '''
                                
                                ''')
		self.assertTrue(vhosts[1].https)

	#def test_sign(self):
	#	str = "Can I Has Cheezburger?"
	#	key = "securekeystring"
	#	sign = self._queryenv._sign(str, key)
	#	self.assertEqual(sign,"fCNPytSqqOy8QTI5L+nZ9AzRMzs=")
		
	#def test_get_canonical_string(self):
	#	dict = {2:"two",3:"three",1:"one",4:"four"}
	#	str = self._queryenv._get_canonical_string(dict)
	#	self.assertEqual(str,"1one2two3three4four")
	
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
