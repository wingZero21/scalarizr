'''
Created on Sep 27, 2010

@author: marat
'''
from pyasn1.type import constraint

( Bits, Counter32, Integer32, Integer32, ModuleIdentity, MibIdentifier, NotificationType, MibScalar, MibTable, MibTableRow, MibTableColumn, Opaque, TimeTicks, enterprises, ) = mibBuilder.importSymbols("SNMPv2-SMI", "Bits", "Counter32", "Integer32", "Integer32", "ModuleIdentity", "MibIdentifier", "NotificationType", "MibScalar", "MibTable", "MibTableRow", "MibTableColumn", "Opaque", "TimeTicks", "enterprises")
(DisplayString, ) = mibBuilder.importSymbols('SNMPv2-TC', 'DisplayString')

'''
class Float(Opaque):
    subtypeSpec = Opaque.subtypeSpec+constraint.ValueSizeConstraint(7,7)
    fixedLength = 7
    pass
'''


#mibBuilder.exportSymbols("SCALING-METRICS-MIB", PYSNMP_MODULE_ID=scalr)

# Types
#mibBuilder.exportSymbols("SCALING-METRICS-MIB", Float=Float)

#mibBuilder.exportSymbols("SCALING-METRICS-MIB", scalr=scalr, mtxTable=mtxTable, mtxEntry=mtxEntry, mtxIndex=mtxIndex, mtxId=mtxId, mtxName=mtxName, mtxValue=mtxValue, mtxError=mtxError)






