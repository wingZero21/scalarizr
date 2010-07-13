from sys import version
from time import time
from pysnmp import majorVersionId
import os, re
import UserDict
from pyasn1.type import constraint, namedval
from pysnmp.smi import error

( DisplayString,) = mibBuilder.importSymbols("SNMPv2-TC", "DisplayString")

( MibScalarInstance, MibTableRow, MibTable, MibTableColumn, Integer32) = mibBuilder.importSymbols('SNMPv2-SMI','MibScalarInstance', 'MibTableRow' , 'MibTable', 'MibTableColumn', 'Integer32')


class NewMibTable(MibTable):
	
	def __init__(self, name):
		MibTable.__init__(self, name)
	
	def getNextNode(self, name, idx):
		mibBuilder.lastBuildId += 1
		mibBuilder.mibSymbols['__UCD-DISKIO-MIB'] = values()
		return MibTable.getNextNode(self, name, idx)
	
NewdiskIOTable = NewMibTable((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1))
diskIOEntry = MibTableRow((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1)).setIndexNames((0, "UCD-DISKIO-MIB", "diskIOIndex"))
diskIOIndex = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 1), Integer32().subtype(subtypeSpec=constraint.ValueRangeConstraint(0, 65535))).setMaxAccess("readonly")
diskIODevice = MibTableColumn((1, 3, 6, 1, 4, 1, 2021, 13, 15, 1, 1, 2), DisplayString()).setMaxAccess("readonly")

def values():
	devicelist = dict(diskIOTable = NewdiskIOTable,
					  diskIOIndex = diskIOIndex, 
					  diskIOEntry = diskIOEntry,
					  diskIODevice = diskIODevice)
	
	blockDevices = []
	for device in os.listdir('/sys/block/'):
		blockDevices.append(device)
		subDevices = os.listdir('/sys/block/' + device)
		for subDevice in subDevices:
			if re.match("^[hs]d[a-z]\d+", subDevice):
				blockDevices.append(subDevice)			
	
	for index in range(len(blockDevices)):
		diskIOIndexInst		= MibScalarInstance(diskIOIndex.getName(), (int(index)+1 ,), diskIOIndex.getSyntax().clone(int(index)+1))
		diskIODeviceInst    = MibScalarInstance(diskIODevice.getName(), (int(index)+1,), diskIODevice.getSyntax().clone(blockDevices[index]))
		devicelist['diskIOIndex' + str(index)] = diskIOIndexInst
		devicelist['diskIODevice' + str(index)] = diskIODeviceInst
	return devicelist

mibBuilder.mibSymbols["__UCD-DISKIO-MIB"] = values()
