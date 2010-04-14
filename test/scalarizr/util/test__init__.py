'''
@author: shaitanich
'''
import unittest


import threading
import time
from scalarizr.util import LocalObject


class TestLocalObject(unittest.TestCase):
	
	def setUp(self):
		self.localobj = LocalObject(Foo)
		
	def tearDown(self):
		del self.localobj
	
	def test_get_from_different_threads(self):
		o_main = self.localobj.get()
		o_thread = None
		
		def run():
			o_thread = self.localobj.get()
		
		t = threading.Thread(target=run)
		t.start()
		t.join()
		
		self.assertNotEqual(o_main, o_thread) 
	
	def test_get_from_the_same_threads(self):
		o1 = self.localobj.get()
		o2 = self.localobj.get()
		self.assertEqual(o1, o2)

"""	
	def test_get(self):
		self.run_func() #called from main thread
		temp = self.storage 
		
		test_runner = threading.Thread(target=self.run_func) #called from another thread
		test_runner.start()
		test_runner.join()
		self.assertNotEqual(temp,self.storage)# bazinga! they are different
		#self.assertNotEqual(temp.prop,self.storage.prop) 		
		test_runner2 = threading.Thread(target=self.run_func2)
		test_runner2.start()
		test_runner2.join()
	 		
	def run_func(self):
		self.storage = LocalObject(Foo).get()
		
	def run_func2(self):
		obj = LocalObject(Foo).get() #calling object once
		obj2 = LocalObject(Foo).get() # calling object secondly
		self.assertEqual(obj.prop,obj2.prop)
		self.assertEqual(obj,obj2)  #hooray! they are the same	
"""		
		
		

class Foo:
	def __init__(self):
		self.prop = threading.currentThread()
	
if __name__ == "__main__":
	
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()