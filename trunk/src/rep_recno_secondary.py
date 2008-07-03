"""TestCases for distributed transactions.
"""

import os
import time
import unittest
import cPickle as pickle

from bsddb3 import db
#----------------------------------------------------------------------

LOCAL_PATH = "test_master"
REMOTE_PATH = "test_client"

CLIENT_PORT = 9000
MASTER_PORT = 9001

CLIENT_HOST = "127.0.0.1"
MASTER_HOST = "127.0.0.1"

master_env = db.DBEnv()
client_env = db.DBEnv()

master_env.open(LOCAL_PATH, db.DB_CREATE | db.DB_INIT_TXN
        | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_LOCK |
        db.DB_INIT_REP | db.DB_RECOVER | db.DB_THREAD, 0666)

client_env.open(REMOTE_PATH, db.DB_CREATE | db.DB_INIT_TXN
        | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_LOCK |
        db.DB_INIT_REP | db.DB_RECOVER | db.DB_THREAD, 0666)
        
# setup host
master_env.repmgr_set_local_site(MASTER_HOST, MASTER_PORT)
master_env.repmgr_add_remote_site(CLIENT_HOST, CLIENT_PORT)

# setup client
client_env.repmgr_set_local_site(CLIENT_HOST, CLIENT_PORT)
client_env.repmgr_add_remote_site(MASTER_HOST, MASTER_PORT)

# set number of sites to replicate
master_env.rep_set_nsites(2)
client_env.rep_set_nsites(2)

# set priorities
master_env.rep_set_priority(10)
master_env.repmgr_set_ack_policy(db.DB_REPMGR_ACKS_ALL)

client_env.rep_set_priority(0)
# set transaction replication policy
client_env.repmgr_set_ack_policy(db.DB_REPMGR_ACKS_ALL)

confimed_master = False
confirmed_startup = False

def confirmed_master(a,b,c) :
  if (b == db.DB_EVENT_REP_MASTER) or (b == db.DB_EVENT_REP_ELECTED) :
    global confirmed_master
    confirmed_master = True
    print "confirmed master"  

def client_startupdone(a,b,c) :
  if b == db.DB_EVENT_REP_STARTUPDONE :
    global confirmed_startup
    confirmed_startup = True
    print "started up"

# setup events so we know when this sthing is started
master_env.set_event_notify(confirmed_master)
client_env.set_event_notify(client_startupdone)


# start the replication manager
master_env.repmgr_start(1, db.DB_REP_MASTER);
client_env.repmgr_start(1, db.DB_REP_CLIENT);


# wait for the clients to start
#while(not confirmed_startup or not confirmed_master):
while( not confirmed_master ):
  time.sleep(0.2)

class testItem:
  def __init__(self):
    self.timestamp = time.time()
    self.type      = "cool_dude"
    self.device_id = 1
    self.user      = 1
    self.x         =1
    self.y         =1
    self.z         =1

  def to_pickle(self):
    return pickle.dumps(self)

print "started"

def floatCompare(key1, key2):
  if(key1 ==''):
    key1 = 0
  if(key2 == ''):
    key2 = 0
  val = float(key1) - float(key2)
  if(val > 0):
    return 1
  if(val < 0):
    return -1
  return 0


# create the DB
master=db.DB(master_env)
txn=master_env.txn_begin()
master.open("test", db.DB_RECNO, db.DB_CREATE , 0666, txn=txn)


# setup the secondary DB: time
timeDB = db.DB(master_env)
timeDB.set_flags(db.DB_DUPSORT)
timeDB.set_bt_compare(floatCompare)
timeDB.open("time_index", db.DB_BTREE, db.DB_CREATE | db.DB_THREAD, txn=txn)

# setup the secondary DB: userDB
userDB = db.DB(master_env)
userDB.set_flags(db.DB_DUPSORT)
userDB.open("user_index", db.DB_BTREE, db.DB_CREATE | db.DB_THREAD, txn=txn)

# setup the secondary DB: userDB
xyzDB = db.DB(master_env)
xyzDB.set_flags(db.DB_DUPSORT)
xyzDB.open("xyz_index", db.DB_BTREE, db.DB_CREATE | db.DB_THREAD, txn=txn)

# committ the creation of the DBs
txn.commit()


# indexer functions
def getTime(priKey, priData):
#  print 'getGenre key: %r data: %r' % (priKey, priData)
  pdata = pickle.loads(priData)
  return str(pdata.timestamp)

def getUser(priKey, priData):
  #print 'getGenre key: %r data: %r' % (priKey, priData)
  pdata = pickle.loads(priData)
  return str(pdata.user)
  
def getXYZ(priKey, priData):
  priData = pickle.loads(priData)
  if(priData.x and priData.y and priData.z):
    key = str(priData.x) + "|" + str(priData.y) + "|" + str(priData.z)
    print key
    return key
  else:
     return db.DB_DONOTINDEX 
  
# associate the tables.
txn=master_env.txn_begin()
master.associate(xyzDB,  getXYZ, db.DB_CREATE, txn=txn)
master.associate(timeDB, getTime, db.DB_CREATE, txn=txn)
master.associate(userDB, getUser, db.DB_CREATE, txn=txn)
txn.commit()

print "Pre Append"
# append an entry

txn=master_env.txn_begin()
recno = master.append(testItem().to_pickle())
txn.commit()
print "Post append"


# make sure the client is started:
print "Waiting on client startup"
timeout=time.time()+1
while((not confirmed_startup or not confirmed_master )and time.time() < timeout):
#while( not confirmed_master):
  time.sleep(0.2)



# connect client:
client = db.DB(client_env)
while True :
    txn=client_env.txn_begin()
    try :
        client.open("test", db.DB_RECNO, db.DB_RDONLY,
                mode=0666, txn=txn)
    except db.DBRepHandleDeadError :
        txn.abort()
        client.close()
        client=db.DB(client_env)
        continue

    txn.commit()
    break

# now read from the client to see if it was replicated
timeout=time.time()+2
v=None
while (time.time()<timeout) and (v==None) :
    v=client.get(recno, None)

print v

# now see if I can query the secondary indexes correctly.
c = xyzDB.cursor()
vals = c.set('1|1|1')
while(vals):
  print vals
  vals =  c.next_dup()

c.close()

master.close()
client.close()

#close the client ENV
master_env.close()
client_env.close()


exit()
