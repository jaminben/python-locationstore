"""
TestCases for DB.associate.
"""

import sys, os, string
import time
from pprint import pprint

import unittest
from test_all import verbose, have_threads, get_new_environment_path

from bdb_interface import BDB_Replicated, BDB_Simple, SimpleLogDB

try:
    # For Pythons w/distutils pybsddb
    from bsddb3 import db, dbshelve
except ImportError:
    # For Python 2.3
    from bsddb import db, dbshelve

try:
    from bsddb3 import test_support
except ImportError:
    from test import test_support


#----------------------------------------------------------------------

class testItem:
  def __init__(self):
    self.timestamp = time.time()
    self.type      = "cool_dude"
    self.device_id = 1
    self.user      = 1
    self.x         = 1
    self.y         = 1
    self.z         = 1

  def to_pickle(self):
    return pickle.dumps(self)


#----------------------------------------------------------------------

class BenInterfaceTestCase(unittest.TestCase):

  def setUp(self):
      self.filename = self.__class__.__name__ + '.db'
      self.homeDir = get_new_environment_path()
      self.a = None
      
  def tearDown(self):
      if(self.a):
        self.a.close()
        
      test_support.rmtree(self.homeDir)

  def testSimpleLocal(self):
    print self.homeDir
    self.a = SimpleLogDB()
    self.a.open(self.homeDir)
  
    # do some stuff here
    input_data = {'bob' : 1, 'rad' : 2, 'timestamp' : time.time() }
    
    rec_num = self.a.append( input_data )
    
    back= self.a.get(rec_num)
    
    assert (back['bob'] == input_data['bob'] )
    
    self.a.close()
  
  def testReplicated(self):
    print self.homeDir
    
    REMOTE_HOST = "127.0.0.1"
    LOCAL_HOST  = "127.0.0.1"
    LOCAL_PORT  = 9000
    REMOTE_PORT = 9001
    
    br = BDB_Replicated( LOCAL_HOST, LOCAL_PORT, True, 10, [ [REMOTE_HOST,REMOTE_PORT] ] )
    
    self.a = SimpleLogDB( br )
    self.a.open(self.homeDir)
  
    # do some stuff here
    input_data = {'bob' : 1, 'rad' : 2, 'timestamp' : time.time() }
    
    rec_num = self.a.append( input_data )
    
    back= self.a.get(rec_num)
    
    assert (back['bob'] == input_data['bob'] )
    
    self.a.close()  

#----------------------------------------------------------------------

def test_suite():
    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(BenInterfaceTestCase))


    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
