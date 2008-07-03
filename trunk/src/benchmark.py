"""
Benchmarks 3 options

MYSQL
BDB (no replication)
BDB with replication
"""
import sys, os, string
import time

from bdb_interface import BDB_Replicated, BDB_Simple, SimpleLogDB
from test_all import verbose, have_threads, get_new_environment_path
from bsddb3 import test_support

PATH = 


#
#
#
def bdb_local_test(dir, packet, count ):
  
  print self.homeDir
  a = SimpleLogDB()
  a.open(dir)

  # do some stuff here
  
  for i in xrange(0,count):
    a.append( input_data )

  a.close()


def run_test( func, args ):
  _start = time.time()
  func(*args)
  return time.time() - _start


results = []

input_data = {'bob' : 1, 'rad' : 2, 'timestamp' : time.time() }
count = 1000

res = run_test([get_new_environment_path(), input_data, 1000])
  
results.append( ("local_test", res) )

#
# 
#

res = run_test( bdb_local_test,  [get_new_environment_path(), input_data, count])
print " Test 1: %f %i" %(res, count )
results.append( ("local_test", res) )





  
bdb_local_test