'''
Created on Jun 4, 2010

@author: marat
'''


# Command Responder
from pysnmp.entity import engine, config
from pysnmp.carrier.asynsock.dgram import udp
#from pysnmp.carrier.asynsock.dgram import udp6
from pysnmp.entity.rfc3413 import cmdrsp, context
from socket import socket





class SnmpServer():
	port = None
	_security_name = None
	_community_name = None
	_engine = None 
	
	def __init__(self, port=None, security_name=None, community_name=None):
		self.port = port
		self._security_name = security_name
		self._community_name = community_name
	
	def start(self):
		if self._engine is None:
			# Create SNMP engine with autogenernated engineID and pre-bound
			# to socket transport dispatcher
			self._engine = engine.SnmpEngine()
			
			# Setup UDP over IPv4 transport endpoint
			config.addSocketTransport(
			    self._engine,
			    udp.domainName,
			    udp.UdpSocketTransport().openServerMode((socket.gethostname(), self.port))
			    )
			
			# Create and put on-line my managed object
			sysDescr, = self._engine.msgAndPduDsp.mibInstrumController.mibBuilder.importSymb', 'sysDescr')
			MibScalarInstance, = self._engine.msgAndPduDsp.mibInstrumController.mibBuilder.importSymbols('SNMPv2-SMI', 'MibScalarInstance')
			sysDescrInstance = MibScalarInstance(
			    sysDescr.name, (0,), sysDescr.syntax.clone('Scalarizr SNMP Command Responder')
			    )
			self._engine.msgAndPduDsp.mibInstrumController.mibBuilder.exportSymbols('PYSNMP-EXAMPLE-MIB', sysDescrInstance=sysDescrInstance)  # creating MIB
			
			# v1/2 setup
			config.addV1System(self._engine, self._security_name, self._community_name)
			
			# VACM setup
			config.addContext(self._engine, '')
			config.addRwUser(self._engine, 1, self._security_name, 'noAuthNoPriv', (1,3,6)) # v1
			config.addRwUser(self._engine, 2, self._security_name, 'noAuthNoPriv', (1,3,6)) # v2c
			
			# SNMP context
			snmpContext = context.SnmpContext(self._engine)
			
			# Apps registration
			cmdrsp.GetCommandResponder(self._engine, snmpContext)
			cmdrsp.SetCommandResponder(self._engine, snmpContext)
			cmdrsp.NextCommandResponder(self._engine, snmpContext)
			cmdrsp.BulkCommandResponder(self._engine, snmpContext)

			
		# Start server
		self._engine.transportDispatcher.jobStarted(1)
		self._engine.transportDispatcher.runDispatcher()			
	
	def stop(self):
		pass