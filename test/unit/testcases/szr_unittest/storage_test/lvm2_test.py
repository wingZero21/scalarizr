'''
Created on Nov 19, 2010

@author: shaitanich
'''
import unittest

from scalarizr.storage import mkloop
from scalarizr.storage.lvm2 import Lvm2
from scalarizr.util import system2

from random import randint

class Test(unittest.TestCase):

	PH_SIZE = 100
	SNAP_SIZE = 10

	def setUp(self):
		self.vg0_name = 'vgtest'
		self.lv0_name = 'data'
		self.file_name = '/tmp/ph%s' % randint(11, 99) 		
		self.ph_device = mkloop(self.file_name, size=self.PH_SIZE)
		self.vg0 = self.lv0 = self.lvs = None	
		self.lvm = Lvm2()
		

		'''
		self.ph_device = '/dev/loop1'
		self.loop_file = 'test.img'
		
		if not os.path.exists(self.loop_file):
			cmd = 'dd if=/dev/zero of=%s bs=1024 count=100000'%self.loop_file
			system(cmd.split(), shell=False)
		
		system('losetup %s %s'% (self.ph_device, self.loop_file))
		self.lvm = Lvm2(self.group_name)
		'''

	def tearDown(self):
		if self.lvs:
			system2('/sbin/lvremove -f %s' % self.lvs, shell=True)
		if self.lv0:
			system2('/sbin/lvremove -f %s' % self.lv0, shell=True)
		if self.vg0:
			system2('/sbin/vgremove -f %s' % self.vg0, shell=True)
		system2('/sbin/pvremove -f %s' % self.ph_device, shell=True)
		system2('/sbin/losetup -d %s' % self.ph_device, shell=True)		
		pass

	def _test_parse_table(self):
		for cmd, length in {'lvs':10, 'vgs':7, 'pvs':6}.items():
			table = self.lvm._parse_table(cmd)
			print table
			self.assertEquals(len(table[0]), length)
	
	def test_all(self):
		# Create PV0
		self.lvm.create_pv(self.ph_device)
		pvi = self.lvm.pv_info(self.ph_device)
		
		self.assertEqual(pvi.vg, '')
		self.assertEqual(pvi.size, '%s.00m' % self.PH_SIZE)
		self.assertEqual(pvi.free, '%s.00m' % self.PH_SIZE)
		self.assertEqual(pvi.pv, self.ph_device)

		# Create VG0
		self.vg0 = self.lvm.create_vg(self.vg0_name, (self.ph_device,))
		vgi = self.lvm.vg_info(self.vg0_name)		
		
		self.assertEqual(self.vg0, '/dev/%s' % self.vg0_name)
		self.assertEqual(vgi.vg, self.vg0_name)
		self.assertEqual(int(vgi.num_pv), 1)
		self.assertEqual(int(vgi.num_lv), 0)

		# Create LV0
		self.lv0 = self.lvm.create_lv(self.vg0_name, self.lv0_name, extents='45%VG')
		lv0i = self.lvm.lv_info(self.lv0)
		
		self.assertEqual(lv0i.lv, self.lv0_name)
		self.assertEqual(lv0i.vg, self.vg0_name)
		self.assertFalse(lv0i.origin)
		self.assertFalse(lv0i.snap_pc)


		# Create LV-Snapshot
		self.lvs = self.lvm.create_lv_snapshot(self.lv0, size=self.SNAP_SIZE)
		lvsi = self.lvm.lv_info(self.lvs)

		self.assertEqual(lvsi.origin, self.lv0_name)
		self.assertEqual(lvsi.size, '%s.00m' % 12)

		
		# Remove snapshot
		self.lvm.remove_lv(self.lvs)
		self.assertRaises(LookupError, self.lvm.lv_info, *(self.lvs,))
		self.lvs = None		
		
		# Remove VG0
		self.lvm.remove_vg(self.vg0_name)
		self.vg0 = self.lv0 = None
		self.assertRaises(LookupError, self.lvm.vg_info, *(self.vg0_name,))
		
		self.lvm.remove_pv(self.ph_device)
		

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()