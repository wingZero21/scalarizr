'''
Created on Sep 28, 2010

@author: marat
'''
import unittest, szr_unittest
from threading import Thread, Event
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import xml.etree.ElementTree as ET 

from scalarizr.bus import bus
from scalarizr.snmp.agent import SnmpServer
from scalarizr.util import system
from scalarizr.queryenv import QueryEnvService

import logging, time, re

'''
			  <!--
			  <metric id="1238" name="Test-Execute-Error-Timeout">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  
			  
			  <metric id="1234" name="Test-Read">
			    <path>%(resources_path)s/snmp/metric-getter-test-read</path>
			    <retrieve-method>read</retrieve-method>
			  </metric>
			  <metric id="1235" name="Test-Execute">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1236" name="Test-Execute-Error-ExitCode">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-exitcode</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1237" name="Test-Execute-Error-StdErr">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-stderr</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  -->
'''

class _QueryEnvResponder(BaseHTTPRequestHandler):
	def do_POST(self):
		self.send_response(200)
		self.end_headers()
		self.wfile.write('''<?xml version="1.0" encoding="UTF-8"?>
			<response>
			<metrics>
			  <metric id="1238" name="Test-Execute-Error-Timeout">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1239" name="Test-Execute-Error-Timeout1">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1240" name="Test-Execute-Error-Timeout2">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1241" name="Test-Execute-Error-Timeout3">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1242" name="Test-Execute-Error-Timeout4">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1243" name="Test-Execute-Error-Timeout5">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1244" name="Test-Execute-Error-Timeout6">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1245" name="Test-Execute-Error-Timeout7">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1246" name="Test-Execute-Error-Timeout8">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			  <metric id="1247" name="Test-Execute-Error-Timeout9">
			    <path>%(resources_path)s/snmp/metric-getter-test-execute-error-timeout</path>
			    <retrieve-method>execute</retrieve-method>
			  </metric>
			</metrics>
			</response>			
			''' % dict(resources_path=szr_unittest.RESOURCE_PATH)			
		)
class  _cnf:
	state = 'running'


class TestMtxTableImpl(unittest.TestCase):
	_logger = None
	
	SECURITY_NAME = 'noAuthNoPriv'
	COMMUNITY_NAME = 'public'
	SNMP_PORT = 8114
	OID = '1.3.6.1.4.1.40000.5'
	
	QUERYENV_PORT = 9999
	QUERYENV_HOST = '0.0.0.0'
	
	_snmp_server = None
	_snmp_thread = None
	
	_queryenv_server = None
	_queryenv_thread = None
	
	def setUp(self):
		self._logger = logging.getLogger(__name__)
		self._logger.debug('setUp')
		
		# Create QueryEnv server
		self._queryenv_server = HTTPServer((self.QUERYENV_HOST, self.QUERYENV_PORT), _QueryEnvResponder)
		
		# Start QueryEnv server
		def start_queryenv_server():
			self._queryenv_server.serve_forever()
		self._queryenv_thread = Thread(target=start_queryenv_server)
		self._queryenv_thread.start()
		
		# Create QueryEnv client
		bus.queryenv_service = QueryEnvService(
			'http://%s:%s' % (self.QUERYENV_HOST, self.QUERYENV_PORT), 
			'd599d4eb-24f4-47b0-8924-b20b305e6515', 
			'JJs61qvMBvxU3qHWgUlkLAY/ypI1KVwF0qQynzVHb4B91bAKgSoo2A=='
		)
		
		bus.cnf = _cnf()
		
		# Create SNMP server
		self._snmp_server = SnmpServer(
			self.SNMP_PORT, 
			self.SECURITY_NAME, self.COMMUNITY_NAME,
			('__SCALING-METRICS-MIB', '__UCD-DISKIO-MIB')
		)

		# Start SNMP server in separate thread
		def start_snmp_server():
			self._snmp_server.start()
		self._snmp_thread = Thread(target=start_snmp_server)
		self._snmp_thread.start()
		
		# Wait 1 second
		time.sleep(1)
		
	
	def tearDown(self):
		self._logger.debug('tearDown')
		
		# Destroy QueryEnv server
		self._queryenv_server.shutdown()
		self._queryenv_thread.join()

		# Destroy SNMP server		
		self._snmp_server.stop()
		self._snmp_thread.join()

	def test_values(self):

		out = system('/usr/bin/snmpwalk -t 45 -v 2c -c public localhost:%s %s' % (self.SNMP_PORT, self.OID))[0]
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.4.1 = STRING: "9.0000000"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.4.2 = STRING: "87.1500000"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.4.3 = STRING: "0.0000000"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.5.3 = STRING: "exitcode: 23"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.4.4 = STRING: "0.0000000"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.5.4 = STRING: "Application error taken from stderr"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.4.5 = STRING: "0.0000000"') != -1)
		self.assertTrue(out.find('SNMPv2-SMI::enterprises.40000.5.1.5.5 = STRING: "Timeouted"') != -1)

		

if __name__ == "__main__":
	unittest.main()