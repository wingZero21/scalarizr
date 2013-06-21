__author__ = 'shaitanich'

import unittest
from scalarizr.api import apache


class MyTestCase(unittest.TestCase):
    def test_something(self):
        webserver = apache.ApacheWebServer()


if __name__ == '__main__':
    unittest.main()
