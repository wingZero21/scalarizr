import unittest
import os
import signal
from scalarizr.snmp import agent
from pysnmp.entity.rfc3413.oneliner import cmdgen
from scalarizr.util import init_tests

	
class Test(unittest.TestCase):
	
	def setUp(self):
		self._agent = agent.SnmpServer(8161, 'user', 'public')
		Assertion.testcase = self
		self._pid = os.fork()
		if self._pid == 0:
			self._agent.start()
			
	def tearDown(self):
		if self._pid != 0:
			os.kill(self._pid, signal.SIGKILL)
			
	def test_snmp_diskio(self):
		pyvars, vars = getValues((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1))		
		test = Assertion(pyvars, vars)
		test.assertValues()
							
	def test_snmp_memory(self):
		
		pyvars, vars = getValues((1, 3, 6, 1, 4, 1, 2021, 4))		
		test = Assertion(pyvars, vars, -4)
		test.assertValues()

	def test_snmp_la(self):		
		pyvars, vars = getValues((1, 3, 6, 1, 4, 1, 2021, 10))
		test = Assertion(pyvars, vars, -3)
		test.assertValues()
		
	def test_snmp_ifstat(self):		
		pyvars, vars = getValues((1, 3, 6, 1, 2, 1, 2))
		test = Assertion(pyvars, vars, -8)
		test.assertValues()
								
def getValues(oid):
		pyerrorIndication, pyerrorStatus, pyerrorIndex, \
		pyvars = cmdgen.CommandGenerator().nextCmd(
			cmdgen.CommunityData('test-agent', 'public'),
			cmdgen.UdpTransportTarget(('localhost', 8161)),
			oid
			)
		
		errorIndication, errorStatus, errorIndex, \
		vars = cmdgen.CommandGenerator().nextCmd(
			cmdgen.CommunityData('test-agent', 'public'),
			cmdgen.UdpTransportTarget(('localhost', 161)),
			oid
			)
		
		return (pyvars, vars)
	
class Assertion():
	def __init__(self, pyvars, vars, places=None):
		self.pyvars = pyvars
		self.vars = vars
		self.places = places
	
	def assertValues(self):
		for varrow in self.vars:
			for name, val in varrow:
				for pyvarrow in self.pyvars:
					for pyname, pyval in pyvarrow:
						if name.prettyPrint() == pyname.prettyPrint():
							if self.places == None:
								self.testcase.assertEqual(val.prettyPrint(), pyval.prettyPrint())
							else:
								try:
									x = int(float(val.prettyPrint()))
									self.testcase.assertAlmostEqual(x, int(float(pyval.prettyPrint())), self.places)
								except ValueError:
									self.testcase.assertEqual(val.prettyPrint(), pyval.prettyPrint())
							

if __name__ == "__main__" :
	init_tests()
	unittest.main()
	
	

		