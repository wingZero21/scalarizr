'''
Created on Oct 14, 2010

@author: marat
'''
from scalarizr.util import PeriodicalExecutor, system2, PopenError

import szr_unittest

import unittest
import time



class TestSystem2(unittest.TestCase):
	def setUp(self):
		pass
	def tearDown(self):
		pass

	def test_raise_exception(self):
		cmd = ('ls', '-la', '/no-exists')
		
		self.assertRaises(PopenError, system2, *(cmd,), **{'error_text' :'ls failed'})
		
		class MyCustomError(PopenError):
			pass
		self.assertRaises(MyCustomError, system2, *(cmd,), **{'exc_class' : MyCustomError})

	def test_exec(self):
		cmd = ('ls', '-la', '/')
		out, err = system2(cmd)[0:2]
		self.assertTrue(len(out) > 0)
		self.assertFalse(len(err))
		
	def test_input_as_string(self):
		cmd = 'cat'
		out = system2(cmd, stdin='/')[0]
		self.assertEqual(out, '/')
		
	def test_int_in_args(self):
		cmd = ('find', '/', '-maxdepth', 1, '-type', 'd', '-name', 'tmp')
		out = system2(cmd)[0].strip()
		self.assertEqual(out, '/tmp')


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
		self._executor.add_task(self.task1, 2, 'Test task 1')
		self._executor.add_task(self.task2, 1.5, 'Test task 2')
		self._executor.add_task(self.task_with_error, 3, 'Test task with error')

		time.sleep(5)
		self.assertEqual(self.counter1, 2)
		self.assertEqual(self.counter2, 3)
	
	def task1(self):
		self.counter1 += 1
		
	def task2(self):
		self.counter2 += 1
		
	def task_with_error(self):
		list + dict


if __name__ == "__main__":
	szr_unittest.main()
	unittest.main()