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
from bsddb3 import test_support, db


import MySQLdb
import cPickle as pickle

#
#
#
def bdb_local_test(a, br,packet, count ):
  
  # do some stuff here
  
  for i in xrange(0,count):
    a.append( packet )

def bdb_local_btree_test(a, br, packet, count ):
  
  # do some stuff here
  
  txn = br.env.txn_begin()
  for i in xrange(0,count):
    #a.put("user",  packet, None,db.DB_KEYLAST) 
    a.put("user",  packet, txn=txn )
  txn.commit()

def bdb_get_handle(dir):
  a = SimpleLogDB()
  a.open(dir)
  return a

def bdb_minimal_handle(dir):
  #| db.DB_INIT_TXN
  #           | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_LOCK |
  #             db.DB_RECOVER | db.DB_THREAD)

  if(False):
    #using BTREE
    br1 = BDB_Simple()
    br1.open(dir)
    dd   = db.DB( br1.env )

   # txn = br1.env.txn_begin()
    dd.set_flags(db.DB_DUP)
    dd.open("logs", db.DB_BTREE, br1.getFlags() | db.DB_INIT_TXN | db.DB_INIT_MPOOL, 0666)
   #3 txn.commit()

    
    return (dd, br1)
   
  # simple
  br1 = BDB_Simple()
  dd = br1.open(dir)
  dd   = db.DB( br1.env )

  txn = br1.env.txn_begin()
  dd.open("logs", db.DB_RECNO, br1.getFlags(), 0666, txn=txn)
  txn.commit()

  # ok, now add the rest
  txn=br1.env.txn_begin()
  timeDB = db.DB(br1.env)
  timeDB.set_flags(db.DB_DUPSORT)
  #timeDB.set_bt_compare(ld.floatCompare)
  timeDB.open("time_index", db.DB_BTREE, br1.getFlags(), txn=txn)

  def getTime(key, data):
    return key 

  dd.associate(timeDB, getTime, db.DB_CREATE, txn=txn)
  txn.commit()
  
  return (dd, br1)
   # really fast:
  env = db.DBEnv()
  env.open(dir, db.DB_INIT_TXN | db.DB_CREATE| db.DB_INIT_MPOOL | db.DB_THREAD | db.DB_RECOVER | db.DB_INIT_LOG, 0666)
  dd   = db.DB( env )
  dd.open("logs", db.DB_RECNO, db.DB_CREATE | db.DB_THREAD, 0666)
  # let's try this:
  return dd
 
  # really fast:
  env = db.DBEnv()
  env.open(dir, db.DB_CREATE| db.DB_INIT_MPOOL, 0666)
  dd   = db.DB( env )
  dd.open("logs", db.DB_RECNO, db.DB_CREATE, 0666)
  
  return dd



def mysql_create_table(kwargs, table_name, database_name):
  table_drop_code = "DROP TABLE IF EXISTS `%s`;" % (table_name)
  table_code = """
  CREATE TABLE `%s` (
  `id` int(11) NOT NULL auto_increment,
  `timestamp` DECIMAL(16,2) default NULL,
  `data` TEXT default NULL,
  PRIMARY KEY  (`id`),
  KEY `timestamp_key` (`timestamp`)
) ENGINE=MyISAM AUTO_INCREMENT=98 DEFAULT CHARSET=latin1;
  """ % (table_name)

  dbh = MySQLdb.connect(**kwargs)
  c = dbh.cursor()
  c.execute("create database " + database_name)
  c.execute("use " + database_name)
  print table_code
  c.execute(table_drop_code)
  c.execute(table_code)
  c.close()
  dbh.close()

def mysql_drop_table(kwargs, table_name, database_name):
  dbh = MySQLdb.connect(**kwargs)
  c = dbh.cursor()
  #c.execute("drop table " + table_name)
  c.execute("drop database " + database_name)
  c.close()
  dbh.close()

def mysql_get_dbh( kwargs ):
  dbh = MySQLdb.connect(**kwargs)
  return dbh

  

def mysql_test( c, table_name, packet, count):
  print packet['timestamp']
  print str(packet['timestamp'])
  for i in xrange(0,count):
    sql = "insert into " + table_name + " (`timestamp`, data) values(" + str(packet['timestamp']) + ", \""+ pickle.dumps(packet) + "\")"    
    #c.execute("insert into " + table_name + " (`timestamp`, data) values(" + str(packet['timestamp']) + ", %s)" ,( pickle.dumps(packet)))  
    #print sql
    c.execute(sql)

def run_test( func, args ):
  _start = time.time()
  func(*args)
  return time.time() - _start


results = []

input_data = {'bob' : 1, 'rad' : 2, 'timestamp' : time.time() }
count = 10000

#
# 
#

#
# Run the tests:
#i
if(False):
  dir = get_new_environment_path()
  hn = bdb_get_handle(dir) 

  res = run_test( bdb_local_test,  [hn, input_data, count])
  print "Took: %f for %i local BDB INSERTS" %(res, count )
  results.append( ("local_test", res) )

  hn.close()
  test_support.rmtree(dir)

# minimal BDB:
#i
if(False):
  dir = get_new_environment_path()
  (hn, br) = bdb_minimal_handle(dir) 

  #print hn.stat()

  res = run_test( bdb_local_btree_test,  [hn, br, pickle.dumps(input_data), count])
  print "Took: %f for %i local BDB INSERTS (minimal)" %(res, count )
  results.append( ("local_test", res) )

  hn.close()
  test_support.rmtree(dir)

if(True):
  dir = get_new_environment_path()
  (hn, br) = bdb_minimal_handle(dir) 

  #print hn.stat()

  res = run_test( bdb_local_test,  [hn, br, pickle.dumps(input_data), count])
  print "Took: %f for %i local BDB INSERTS (minimal)" %(res, count )
  results.append( ("local_test", res) )

  hn.close()
  test_support.rmtree(dir)



# setup the mysql
mysql_args = {'host':'localhost', 'user': 'root' }
try:
  mysql_drop_table(mysql_args, 'insert_test', 'speed_test')
except:
  pass
mysql_create_table(mysql_args, 'insert_test', 'speed_test')
mysql_args['db'] = 'speed_test'

dbh = mysql_get_dbh(mysql_args)
c   = dbh.cursor()

res = run_test( mysql_test,  [c, 'insert_test', input_data, count])
print "Took: %f for %i local mysql INSERTS" %(res, count )
results.append( ("mysql_test", res) )

c.close()
dbh.close()
#mysql_drop_table(mysql_args, 'insert_test', 'speed_test')


  
