'''
Created on Jun 4, 2010

@author: marat
'''

# Command Responder
from pysnmp.entity import engine, config
from pysnmp.carrier.asynsock.dgram import udp
#from pysnmp.carrier.asynsock.dgram import udp6
from pysnmp.entity.rfc3413 import cmdrsp, context
import socket
#from pysnmp.smi import builder
import os, re

#mibBuilder = builder.MibBuilder()
#Integer32, = mibBuilder.importSymbols('SNMPv2-SMI', 'Integer32')

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
			
			mibBuilder = self._engine.msgAndPduDsp.mibInstrumController.mibBuilder
			
			#mibBuilder.
			MibSources = mibBuilder.getMibPath()
			
			sources =  ['/mibs','/mibs/instances']
			for source in sources:
				MibSources += ( (os.path.realpath(os.path.dirname(__file__) + source), ))
			apply(mibBuilder.setMibPath, MibSources)
			
			mibBuilder.loadModules('__UCD-SNMP-MIB')

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
#		udp.UdpSocketTransport().handle_close()
		self._engine.transportDispatcher.closeDispatcher()


