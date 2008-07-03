from bdb_interface import BDB_Replicated, BDB_Simple, SimpleLogDB
from bsddb3 import db
import time

REMOTE_HOST = "127.0.0.1"
LOCAL_HOST  = "127.0.0.1"
LOCAL_PORT  = 9000
REMOTE_PORT = 9001

REMOTE_PORT2 = 9002
br = BDB_Replicated( LOCAL_HOST, LOCAL_PORT, True, 10, [ [REMOTE_HOST,REMOTE_PORT], [REMOTE_HOST, REMOTE_PORT2] ] )

if(False):
  br.open("test_dir1")

  d   = db.DB( br.env )

  txn = br.env.txn_begin()
  d.open("logs", db.DB_RECNO, db.DB_CREATE | db.DB_THREAD, 0666, txn=txn)
  txn.commit()

a = SimpleLogDB( br )
a.open("test_dir1")


if(True):
  print "IN HERE"

  br1 = BDB_Replicated( REMOTE_HOST, REMOTE_PORT, False, 10, [ [LOCAL_HOST,LOCAL_PORT] , [REMOTE_HOST, REMOTE_PORT2] ] )
  dd = br1.open("test_dir2")
  dd   = db.DB( br1.env )

  br.wait_on_ready()
  br1.wait_on_ready()

  print "OPEN LOGS"
  txn = br1.env.txn_begin()
  dd.open("logs", db.DB_RECNO, br1.getFlags(), 0666, txn=txn)
  txn.commit()
  print "LSd"
  
  ld = SimpleLogDB( br1)

  # setup the secondary DB: time
  txn=br1.env.txn_begin()
  timeDB = db.DB(br1.env)
  timeDB.set_flags(db.DB_DUPSORT)
  timeDB.set_bt_compare(ld.floatCompare)
  timeDB.open("time_index", db.DB_BTREE, br1.getFlags(), txn=txn)
  txn.commit()

  # setup the secondary DB: userDB
  txn=br1.env.txn_begin()
  userDB = db.DB(br1.env)
  userDB.set_bt_compare(ld.intCompare)
  userDB.set_flags(db.DB_DUPSORT)
  userDB.open("user_index", db.DB_BTREE, br1.getFlags() , txn=txn)
  txn.commit()

  # setup the secondary DB: locationDB
  txn=br1.env.txn_begin()
  xyzDB = db.DB(br1.env)
  xyzDB.set_flags(db.DB_DUPSORT)
  xyzDB.open("xyz_index", db.DB_BTREE, br1.getFlags(), txn=txn)
  txn.commit()

  # setup the secondary DB: deviceDB
  txn=br1.env.txn_begin()
  deviceDB = db.DB(br1.env)
  deviceDB.set_flags(db.DB_DUPSORT)
  deviceDB.open("device_index", db.DB_BTREE, br1.getFlags() , txn=txn)
  txn.commit()

  # commit the creation of the DBs


  # associate the tables.
  txn=br1.env.txn_begin()
  dd.associate(xyzDB,  ld.getXYZ,  db.DB_CREATE, txn=txn)
  dd.associate(timeDB, ld.getTime, db.DB_CREATE, txn=txn)
  dd.associate(userDB, ld.getUser, db.DB_CREATE, txn=txn)
  dd.associate(deviceDB, ld.getDevice, db.DB_CREATE, txn=txn)
  txn.commit()


else:
  br1 = BDB_Replicated( REMOTE_HOST, REMOTE_PORT, False, 10, [ [LOCAL_HOST,LOCAL_PORT] , [REMOTE_HOST, REMOTE_PORT2] ] )

  b = SimpleLogDB( br1 )
  b.open("test_dir2")


# ok down here I can append stuff
print "0--------------------------------------"

#rec1 =  b.append({'user' : 1 , 'timestamp' : time.time() })
print "first"
rec1 =  a.append({'user' : '1' , 'timestamp' : time.time() })
print "2first"
rec1 =  a.append({'dsfdsuser' : '1'  })
print "3first"
rec2 = a.append({'user' : '2' })

#print b.get(rec1)
#print b.get(rec2)
