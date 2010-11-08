'''
Created on Nov 05, 2010

@author: Dmytro Korsakov
'''
import unittest
import os
from szr_unittest import RESOURCE_PATH
from scalarizr.util.filetool import write_file, read_file, split


class TestSplit(unittest.TestCase):
	
	def setUp(self):
		pass

	def tearDown(self):
		pass
	
	def test_split(self):
		bytes = 1024
		path = 'test_split.txt'
		cur_dir = './'
		prefix = '1'
		
		file = open(path, 'w')
		file.write(''.join(['a' for i in range(bytes)]))
		file.close()
		
		split(path,prefix, bytes/2, cur_dir)
		
		chunks = 0
		for fname in os.listdir(cur_dir):
			if fname.startswith(prefix+'.'):
				chunks += 1
				os.remove(os.path.join(cur_dir, fname))
				
		self.assertEqual(chunks, 2)
		
	def test_split_Null(self):
		fname = r'/*not_real_name*/'
		self.assertRaises((OSError,IOError), split, *(fname,'1', 512, '.'))
				
		
class TestFileTool(unittest.TestCase):

	def setUp(self):
		self.testfile = os.path.join(RESOURCE_PATH, 'test.txt')
		self.test_line = 'test_file_read'
		self._cleanup()

	def tearDown(self):
		self._cleanup()

	def test_read(self):
		self._write(self.testfile, self.test_line)
		line = read_file(self.testfile)
		self.assertEqual(line, self.test_line)
		
	def test_read_null(self):
		if os.path.exists(self.testfile):
			raise BaseException
		
		line = read_file(self.testfile)
		self.assertNotEqual(line, self.test_line)
		self.assertEqual(line, None)
		
	def test_write(self):
		write_file(self.testfile, self.test_line)
		line = self._read(self.testfile)
		self.assertEqual(line, self.test_line)
		
	def test_write_to_unexisted_dir(self):
		testfile = os.path.join(RESOURCE_PATH, 'test_dir', 'test.txt')
		write_file(testfile, self.test_line)
		line = read_file(testfile)
		
		os.remove(testfile)
		os.removedirs(os.path.dirname(testfile))
		
		self.assertEqual(line, self.test_line)	
		
	def test_append_file(self):
		self._write(self.testfile, self.test_line)
		write_file(self.testfile, self.test_line, mode='a')
		doubled_line = self._read(self.testfile)
		self.assertEquals(doubled_line, self.test_line + self.test_line)
		
	def test_rewrite_file(self):
		self._write(self.testfile, self.test_line)
		write_file(self.testfile, self.test_line)
		line = self._read(self.testfile)
		self.assertEquals(line, self.test_line)
	
	def test_write_busy_file(self):
		file = open(self.testfile, 'w')
		file.write(self.test_line)
		result = write_file(self.testfile, self.test_line)
		file.close()
		self.assertTrue(result)
		
	def test_read_busy_file(self):
		file = open(self.testfile, 'w')
		file.write(self.test_line)
		result = read_file(self.testfile)
		file.close()
		self.assertNotEqual(result, self.test_line)
		self.assertEqual(result, '')
		
	def test_read_empty_file(self):
		file = open(self.testfile, 'w')
		file.close()
		self.assertTrue(os.path.exists(self.testfile))
		result = read_file(self.testfile)
		self.assertEqual(result, '')
		
	def _read(self, fname):
		file = open(fname, 'r')
		line = file.read()
		file.close()
		return line
	
	def _write(self, fname, line):
		file = open(fname, 'w')
		file.write(line)
		file.close()
	
	def _cleanup(self):
		if os.path.exists(self.testfile):
			os.remove(self.testfile)



if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()