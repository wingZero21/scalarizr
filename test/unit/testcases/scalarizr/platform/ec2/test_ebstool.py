'''
Created on Sep 8, 2010

@author: marat
'''
import unittest, os
from boto import connect_ec2
from ConfigParser import ConfigParser

from scalarizr.platform.ec2 import ebstool
from scalarizr.util import init_tests

class Test(unittest.TestCase):

	cnf = ConfigParser()
	cnf.read(os.path.expanduser('~/.aws.ini'))
	ec2_conn = connect_ec2(cnf.get('access', 'key-id'), cnf.get('access', 'key'))

	def test_wait_snapshot(self):
		vol = snap = None
		try:
			vol = ebstool.create_volume(self.ec2_conn, 10, 'us-east-1d')
			snap = vol.create_snapshot('UnitTest %s snapshot' % __name__)
			ebstool.wait_snapshot(self.ec2_conn, snap)
			self.assertEqual(snap.status, 'completed')
		finally:
			if vol:
				vol.delete();
			if snap:
				snap.delete()


if __name__ == "__main__":
	init_tests()
	unittest.main()