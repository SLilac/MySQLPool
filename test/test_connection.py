#!/usr/bin/python
#-*- coding: utf-8 -*-

"""
Author: Thomas Huang
"""

import unittest
import mysqlpool 
from mock import Mock
try:
    from hashlib import md5 
except Exception, e:
    from md5 import md5

def suite():
    loader = unittest.TestLoader()
    alltests = unittest.TestSuite()
    alltests.addTest(loader.loadTestsFromTestCase(Connection))

    return alltests
#    return unittest.TestLoader().loadTestsFromTestCase(Connection)

class Connection(unittest.TestCase):

    def setUp(self):
        self.user = 'unittest'
        self.passwd = "dRtct45"
        self.host = 'localhost'
        self.db = 'employees'
    
    def testCursor(self):
        pass
    
        
        