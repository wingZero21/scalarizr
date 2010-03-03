'''
Created on Dec 14, 2009

@author: marat
'''
import unittest


class Test(unittest.TestCase):


	def test_crypto(self):
		from scalarizr.util import CryptoUtil
		key = "yyUKYpE1IsMCBdmSAs7FpxWwRszfSbPBqD+Obi3P7Cg28SpLuqB4eA=="
		s = "vtpGn1C4TbvR/Z7P4qVRij/UGYwcB16D5gTR18cOF+AZhgJqSM/LkVXWAMxFWs5NK83EyZ42/6Y0kkpMJgMGw9IrRBawtsrwI8+5Ty1r+5Wi8KsMXY5hbUmujbwghuVOGpWR4WrTQ/kUARo5oE8U196t97jXVp6r7zSgPe94MyGSHw0+EDZzYXS/q1jePoHaZSSD+meyts9KGzMHAAvvN2E1"
		c = CryptoUtil()
		se = c.decrypt(s, key)
		print se
		pass


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()
