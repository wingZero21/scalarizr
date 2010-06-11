from sys import version
from time import time
from pysnmp import majorVersionId
import os, re

( DisplayString,) = mibBuilder.importSymbols("SNMPv2-TC", "DisplayString")



( MibScalarInstance,) = mibBuilder.importSymbols('SNMPv2-SMI','MibScalarInstance')
  
(diskIOTable,
diskIOEntry,
diskIOIndex,
diskIODevice) = mibBuilder.importSymbols(
    'UCD-DISKIO-MIB',
    'diskIOTable',
    'diskIOEntry',
    'diskIOIndex',
    'diskIODevice'
    )

file = open('/proc/partitions', "r")
partitionsList = file.readlines()
file.close()
partitions = {}


subBlockDevices = []
blockDevices = os.listdir('/sys/block')
for device in blockDevices:
	subDevices = os.listdir('/sys/block/' + device)
	for subDevice in subDevices:
		if re.match("^[hs]d[a-z]\d+", subDevice):
			subBlockDevices.append(subDevice)

blockDevices += subBlockDevices

#for index in range(len(blockDevices)):
#	if re.match ('[\s*\d*]{3}', row):
#		partitions[row.split()[1]] = row.split()[3] 
		
for index in range(len(blockDevices)):
	diskIOIndexInst		= MibScalarInstance(diskIOIndex.getName(), (int(index)+1 ,), diskIOIndex.getSyntax().clone(int(index)+1))
	diskIODeviceInst    = MibScalarInstance(diskIODevice.getName(), (int(index)+1,), diskIODevice.getSyntax().clone(blockDevices[index]))

	namedSyms = {
		'diskIOIndex' + str(index) : diskIOIndexInst,
		'diskIODeviceInst' + str(index) : diskIODeviceInst,
	}	
	mibBuilder.exportSymbols("__UCD-DISKIO-MIB", **namedSyms)

mibBuilder.exportSymbols(
    "__UCD-DISKIO-MIB",
    diskIOTable	 = diskIOTable,
    diskIOEntry  = diskIOEntry
    )



