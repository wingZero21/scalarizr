'''
Created on Jun 4, 2010

@author: marat
'''

"""
# Command Responder
from pysnmp.entity import engine, config
from pysnmp.carrier.asynsock.dgram import udp
#from pysnmp.carrier.asynsock.dgram import udp6
from pysnmp.entity.rfc3413 import cmdrsp, context

# Create SNMP engine with autogenernated engineID and pre-bound
# to socket transport dispatcher
snmpEngine = engine.SnmpEngine()

# Setup UDP over IPv4 transport endpoint
config.addSocketTransport(
    snmpEngine,
    udp.domainName,
    udp.UdpSocketTransport().openServerMode(('127.0.0.1', 161))
    )

# Setup UDP over IPv6 transport endpoint
#config.addSocketTransport(
#    snmpEngine,
#    udp6.domainName,
#    udp6.Udp6Transport().openServerMode(('::1', 161))
#    )

# Create and put on-line my managed object
sysDescr, = snmpEngine.msgAndPduDsp.mibInstrumController.mibBuilder.importSymbols('SNMPv2-MIB', 'sysDescr')
MibScalarInstance, = snmpEngine.msgAndPduDsp.mibInstrumController.mibBuilder.importSymbols('SNMPv2-SMI', 'MibScalarInstance')
sysDescrInstance = MibScalarInstance(
    sysDescr.name, (0,), sysDescr.syntax.clone('Example Command Responder')
    )
snmpEngine.msgAndPduDsp.mibInstrumController.mibBuilder.exportSymbols('PYSNMP-EXAMPLE-MIB', sysDescrInstance=sysDescrInstance)  # creating MIB

# v1/2 setup
config.addV1System(snmpEngine, 'test-agent', 'public')

# v3 setup
config.addV3User(
    snmpEngine, 'test-user',
    config.usmHMACMD5AuthProtocol, 'authkey1',
    config.usmDESPrivProtocol, 'privkey1'
    )
    
# VACM setup
config.addContext(snmpEngine, '')
config.addRwUser(snmpEngine, 1, 'test-agent', 'noAuthNoPriv', (1,3,6)) # v1
config.addRwUser(snmpEngine, 2, 'test-agent', 'noAuthNoPriv', (1,3,6)) # v2c
config.addRwUser(snmpEngine, 3, 'test-user', 'authPriv', (1,3,6)) # v3

# SNMP context
snmpContext = context.SnmpContext(snmpEngine)

# Apps registration
cmdrsp.GetCommandResponder(snmpEngine, snmpContext)
cmdrsp.SetCommandResponder(snmpEngine, snmpContext)
cmdrsp.NextCommandResponder(snmpEngine, snmpContext)
cmdrsp.BulkCommandResponder(snmpEngine, snmpContext)
snmpEngine.transportDispatcher.jobStarted(1) # this job would never finish
snmpEngine.transportDispatcher.runDispatcher()

"""

class SnmpServer():
	
	def __init__(self, port=None, security_name=None, community_name=None):
		pass
	
	def start(self):
		pass
	
	def stop(self):
		pass