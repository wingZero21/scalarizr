'''
Created on Oct 14, 2010

@author: marat
'''
from scalarizr.util import PeriodicalExecutor

import unittest
import time

class TestPeriodicalExecutor(unittest.TestCase):

	counter1 = None
	counter2 = None

	def setUp(self):
		self._executor = PeriodicalExecutor()
		self._executor.start()
		self.counter1 = self.counter2 = 0
	
	def tearDown(self):
		self._executor.shutdown()
		del self._executor

	def test_1(self):
		# Execute each 5 seconds
		self._executor.add_task(self.task1, 5, 'Test task 1')
		self._executor.add_task(self.task2, 3, 'Test task 2')
		self._executor.add_task(self.task_with_error, 3, 'Test task with error')

		time.sleep(16)
		self.assertEqual(self.counter1, 3)
		self.assertEqual(self.counter2, 5)
	
	def task1(self):
		self.counter1 += 1
		
	def task2(self):
		self.counter2 += 1
		
	def task_with_error(self):
		list + dict


if __name__ == "__main__":
	import szr_unittest
	unittest.main()