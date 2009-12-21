'''
Created on Dec 14, 2009

@author: marat
'''
import unittest


class Test(unittest.TestCase):


	def test_crypto(self):
		from scalarizr.util import CryptoUtil
		key = "yyUKYpE1IsMCBdmSAs7FpxWwRszfSbPBqD+Obi3P7Cg28SpLuqB4eA=="
		s = "qwerty"
		c = CryptoUtil()
		c.encrypt(s, key)
		pass


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()