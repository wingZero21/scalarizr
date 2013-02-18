'''
Created on Apr 6, 2010

@author: marat
'''
from scalarizr.util import configtool, init_tests
from scalarizr.bus import bus
from ConfigParser import ConfigParser
from scalarizr.linux import mount
import unittest
import shutil
import os


class Test(unittest.TestCase):

	def test_update(self):
		filename = os.path.dirname(__file__) + "/../../resources/platform.ec2.ini"
		shutil.copyfile(filename + ".orig", filename)
		configtool.update(filename, {
			"platform_ec2" : {
				"account_id" : "323232321",
				"key_id" : "^ffdfdfte33ghgbfv",
				"key" : "",
				"new_option" : "vvaalluuee"
			},
			"new_section" : {
				"new_option2" : "vvvaaallluueee2"
			}
		})
		
		config = ConfigParser()
		config.read(filename)
		self.assertTrue(config.has_section("new_section"))
		self.assertTrue(config.has_option("new_section", "new_option2"))
		self.assertEqual(config.get("platform_ec2", "account_id"), "323232321")
		self.assertEqual(config.get("platform_ec2", "key"), "")
		self.assertEqual(config.get("platform_ec2", "key_id"), "^ffdfdfte33ghgbfv")
		self.assertEqual(config.get("platform_ec2", "new_option"), "vvaalluuee")

	def test_mount_private_d(self):
		try:
			privated_path = bus.etc_path+"/private.d"
			img_path = "/mnt/test-privated.img"
			configtool.mount_private_d(privated_path, img_path, 10000)
			self.assertTrue(os.path.exists(privated_path + "/keys/default"))
			mtab = mount.mounts()
			self.assertTrue(mtab.contains(privated_path))
		finally:
			mount.umount(privated_path)
			os.remove(img_path)


if __name__ == "__main__":
	init_tests()
	unittest.main()