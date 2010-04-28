'''
@author: Dmytro Korsakov
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

class Foo:
	def __init__(self):
		self.prop = threading.currentThread()
	
if __name__ == "__main__":

	unittest.main()