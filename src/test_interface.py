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
      self.homeDir1 = get_new_environment_path()
      self.homeDir2 = get_new_environment_path()
            
      self.a = None
      self.b = None
      self.c = None
      
  def tearDown(self):
      if(self.a):
        self.a.close()

      if(self.b):
        self.b.close()
      
      if(self.c):
        self.c.close()
                  
      test_support.rmtree(self.homeDir)
      test_support.rmtree(self.homeDir1)
      test_support.rmtree(self.homeDir2)
      

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

  def testReplicatedComplex(self):
    
    LOCAL_HOST  = "127.0.0.1"
    LOCAL_PORT  = 9003

    REMOTE_HOST = "127.0.0.1"
    REMOTE_PORT = 9008
    
    RREMOTE_HOST = "127.0.0.1"
    RREMOTE_PORT = 9009


    br = BDB_Replicated( LOCAL_HOST, LOCAL_PORT, True, 10, [ [REMOTE_HOST,REMOTE_PORT] ] )
    a = SimpleLogDB( br )
    a.open(self.homeDir)

    brr  = BDB_Replicated( REMOTE_HOST,REMOTE_PORT, False, 10, [ [LOCAL_HOST, LOCAL_PORT]   ] )
    b = SimpleLogDB( brr )
    b.open(self.homeDir1)
    
    #print "POST OPEN COMMAND"
    #self.c = SimpleLogDB( brr2 )
    #self.c.open(self.homeDir2)

    # do some stuff here
    input_data = {'bob' : 1, 'rad' : 2, 'timestamp' : time.time() }
    rec_num = a.append( input_data )

    back= a.get(rec_num)

    assert (back['bob'] == input_data['bob'] )

    #back= self.b.get(rec_num)
    #assert (back['bob'] == input_data['bob'] )

    #back= self.c.get(rec_num)
    #assert (back['bob'] == input_data['bob'] )


    a.close()
    b.close()
    #self.c.close()

  def test_really_simple(self):
    REMOTE_HOST = "127.0.0.1"
    LOCAL_HOST  = "127.0.0.1"
    LOCAL_PORT  = 9000
    REMOTE_PORT = 9001

    REMOTE_PORT2 = 9002
    br = BDB_Replicated( LOCAL_HOST, LOCAL_PORT, True, 10, [ [REMOTE_HOST,REMOTE_PORT], [REMOTE_HOST, REMOTE_PORT2] ] )

    a = SimpleLogDB( br )
#    a.open("test_dir1")
    a.open(self.homeDir)


    br1 = BDB_Replicated( REMOTE_HOST, REMOTE_PORT, False, 10, [ [LOCAL_HOST,LOCAL_PORT] , [REMOTE_HOST, REMOTE_PORT2] ] )

    b = SimpleLogDB( br1 )
    #b.open("test_dir2")
    b.open(self.homeDir1)


    # ok down here I can append stuff
    print "0--------------------------------------"

    #rec1 =  b.append({'user' : 1 , 'timestamp' : time.time() })
    print "first"
    rec1 =  a.append({'user' : '1' , 'timestamp' : time.time() })
    print "2first"
    rec1 =  a.append({'dsfdsuser' : '1'  })
    print "3first"
    rec2 = a.append({'user' : '2' })



#----------------------------------------------------------------------

def test_suite():
    suite = unittest.TestSuite()

    suite.addTest(unittest.makeSuite(BenInterfaceTestCase))


    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='test_suite')
