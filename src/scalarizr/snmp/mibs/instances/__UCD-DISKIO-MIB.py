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

blockDevices = []
for device in os.listdir('/sys/block'):
	blockDevices.append(device)
	subDevices = os.listdir('/sys/block/' + device)
	for subDevice in subDevices:
		if re.match("^[hs]d[a-z]\d+", subDevice):
			blockDevices.append(subDevice)
		
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



