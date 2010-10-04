from sys import version
from time import time
from pysnmp import majorVersionId
import os, re
import UserDict
from pyasn1.type import constraint, namedval
from pysnmp.smi import error
from scalarizr.snmp.mibs import validate

( DisplayString,) = mibBuilder.importSymbols("SNMPv2-TC", "DisplayString")

( MibScalarInstance, MibTableRow, MibTable, MibTableColumn, Integer32, Counter32, Counter64) = mibBuilder.importSymbols('SNMPv2-SMI','MibScalarInstance', 'MibTableRow' , 'MibTable', 'MibTableColumn', 'Integer32',  'Counter32', 'Counter64')

class NewMibTable(MibTable):
	
	def __init__(self, name):
		MibTable.__init__(self, name)
	
	def getNextNode(self, name, idx):
		mibBuilder.lastBuildId += 1
		mibBuilder.mibSymbols['__UCD-DISKIO-MIB'] = values()
		return MibTable.getNextNode(self, name, idx)
	
NewdiskIOTable  = NewMibTable((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1))
diskIOEntry     = MibTableRow((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1)).setIndexNames((0, "UCD-DISKIO-MIB", "diskIOIndex"))
diskIOIndex     = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 1), Integer32().subtype(subtypeSpec=constraint.ValueRangeConstraint(0, 65535))).setMaxAccess("readonly")
diskIODevice    = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 2), DisplayString()).setMaxAccess("readonly")

diskIONRead     = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 3), Counter32()).setMaxAccess("readonly")
diskIONWritten  = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 4), Counter32()).setMaxAccess("readonly")
diskIOReads     = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 5), Counter32()).setMaxAccess("readonly")
diskIOWrites    = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 6), Counter32()).setMaxAccess("readonly")
diskIONReadX    = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 12), Counter64()).setMaxAccess("readonly")
diskIONWrittenX = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 13), Counter64()).setMaxAccess("readonly")

def values():
	devicelist = dict(diskIOTable = NewdiskIOTable,
					  diskIOIndex = diskIOIndex, 
					  diskIOEntry = diskIOEntry,
					  diskIODevice = diskIODevice,
					  diskIONRead  = diskIONRead,
					  diskIONWritten = diskIONWritten,
					  diskIOReads  = diskIOReads,
					  diskIOWrites = diskIOWrites,
					  diskIONReadX = diskIONReadX, 
					  diskIONWrittenX = diskIONWrittenX)

	fp = open('/proc/diskstats')
	diskstats = fp.readlines()
	fp.close()
	
	for index in range(len(diskstats)):
		values = diskstats[index].split()

		devicelist['diskIOIndex' + str(index)]		= MibScalarInstance(diskIOIndex.getName(), (int(index)+1 ,), diskIOIndex.getSyntax().clone(int(index)+1))
		devicelist['diskIODevice' + str(index)]     = MibScalarInstance(diskIODevice.getName(), (int(index)+1,), diskIODevice.getSyntax().clone(values[2]))
		
		devicelist['diskIONRead' + str(index)]      = MibScalarInstance(diskIONRead.getName(), (int(index)+1,), validate(Counter32(), int(values[5])*512))
		devicelist['diskIONWritten' + str(index)]   = MibScalarInstance(diskIONWritten.getName(), (int(index)+1,), validate(Counter32(), int(values[9])*512))
		devicelist['diskIOReads' + str(index)]		= MibScalarInstance(diskIOReads.getName(), (int(index)+1,), validate(Counter32(), values[3]))
		devicelist['diskIOWrites' + str(index)]	    = MibScalarInstance(diskIOWrites.getName(), (int(index)+1,), validate(Counter32(), values[7]))
		devicelist['diskIONReadX' + str(index)]  	= MibScalarInstance(diskIONReadX.getName(), (int(index)+1,), validate(Counter64(), int(values[5])*512))
		devicelist['diskIONWrittenX' + str(index)]	= MibScalarInstance(diskIONWrittenX.getName(), (int(index)+1,), validate(Counter64(), int(values[9])*512)) 
	
	return devicelist


mibBuilder.mibSymbols["__UCD-DISKIO-MIB"] = values()
