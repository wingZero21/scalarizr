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
scalr = ModuleIdentity((1, 3, 6, 1, 4, 1, 36632))
mtxTable = MibTable((1, 3, 6, 1, 4, 1, 36632, 5))
mtxEntry = MibTableRow((1, 3, 6, 1, 4, 1, 36632, 5, 1)).setIndexNames((0, "SCALING-METRICS-MIB", "mtxIndex"))
mtxIndex = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 1), Integer32()).setMaxAccess("readonly")
mtxId = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 2), Integer32()).setMaxAccess("readonly")
mtxName = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 3), DisplayString()).setMaxAccess("readonly")
mtxValue = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 4), DisplayString()).setMaxAccess("readonly")
mtxError = MibTableColumn((1, 3, 6, 1, 4, 1, 36632, 5, 1, 5), DisplayString()).setMaxAccess("readonly")

uglyFeatures = MibIdentifier((1, 3, 6, 1, 4, 1, 36632, 6))
authShutdown = MibScalar((1, 3, 6, 1, 4, 1, 36632, 6, 1), Integer32()).setMaxAccess("readonly")


mibBuilder.exportSymbols("SCALING-METRICS-MIB", PYSNMP_MODULE_ID=scalr)

# Types
#mibBuilder.exportSymbols("SCALING-METRICS-MIB", Float=Float)

mibBuilder.exportSymbols("SCALING-METRICS-MIB", scalr=scalr, mtxTable=mtxTable, mtxEntry=mtxEntry, mtxIndex=mtxIndex, mtxId=mtxId, mtxName=mtxName, mtxValue=mtxValue, mtxError=mtxError, uglyFeatures=uglyFeatures, authShutdown=authShutdown)






