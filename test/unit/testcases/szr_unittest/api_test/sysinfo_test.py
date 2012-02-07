'''
Created on 07.02.2012

@author: sam
'''
import unittest

from scalarizr.api import sysinfo

DISKSTATS = ['   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       1 ram1 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       2 ram2 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       3 ram3 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       4 ram4 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       5 ram5 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       6 ram6 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       7 ram7 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       8 ram8 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1       9 ram9 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1      10 ram10 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1      11 ram11 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1      12 ram12 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1      13 ram13 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1      14 ram14 0 0 0 0 0 0 0 0 0 0 0\n',
	'   1      15 ram15 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       0 loop0 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       1 loop1 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       2 loop2 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       3 loop3 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       4 loop4 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       5 loop5 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       6 loop6 0 0 0 0 0 0 0 0 0 0 0\n',
	'   7       7 loop7 0 0 0 0 0 0 0 0 0 0 0\n',
	'   8       0 sda 122983 61549 5368141 1205624 98504 130221 3727544 2555488 0 841516 3760836\n', 
	'   8       1 sda1 166 28 1328 1060 0 0 0 0 0 1060 1060\n', 
	'   8       2 sda2 162 0 1296 1516 0 0 0 0 0 1516 1516\n', 
	'   8       3 sda3 2 0 12 664 0 0 0 0 0 664 664\n', 
	'   8       5 sda5 113013 43150 5151042 1140888 72407 80384 3194400 2450600 0 788040 3591200\n', 
	'   8       6 sda6 9464 17189 213105 58520 16759 49837 533144 63332 0 48456 121876\n', 
	' 253       0 dm-0 26316 0 210528 284168 66644 0 533144 9233892 0 50344 9518068\n']


class TestSysInfoAPI(unittest.TestCase):
	
	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self, methodName=methodName)

		self.info = sysinfo.SysInfoAPI(diskstats=DISKSTATS)


	def test_block_devices(self):

		devs = self.info.block_devices()

		self.assertEqual(devs, [])
		

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()