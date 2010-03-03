'''
Created on Mar 3, 2010

@author: marat
'''
import unittest


class Test(unittest.TestCase):


	def test_all(self):
		from scalarizr.util import Observable
		o = Observable()
		
		args = ("undefevent", "param1", "param2")
		self.assertRaises(Exception, o.on, *args)

		o.define_events("add", "remove")
		
		# Add listeners
		def handler1(item):
			item["h1"] = True
		def handler2(item):
			item["h2"] = True

		o.on("add", handler1, handler2)
		item = {}
		o.fire("add", item)
		self.assertTrue(item["h1"])
		self.assertTrue(item["h2"])
		
		
		# Remove listener
		o.un("add", handler2)
		item = {}
		o.fire("add", item)
		self.assertTrue(item["h1"])
		self.assertFalse(item.has_key("h2"))


		# Suspend events
		o.suspend_events()
		item = {}
		o.fire("add", item)
		self.assertFalse(item.has_key("h1"))

		
		# Resume events
		o.resume_events()
		item = {}
		o.fire("add", item)
		self.assertTrue(item["h1"])


if __name__ == "__main__":
	unittest.main()
