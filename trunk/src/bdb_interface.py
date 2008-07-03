"""
  This is a very simple BDB Interface
  it is meant to generalize for Replication and non Replication
  
"""

from bsddb3 import db
import time
import cPickle as pickle


class BDB_Replicated:
  " sets up a master server "
  def __init__(self, local_host, local_port, master = False, priority = 10, client_list = []):
    self.local_host = local_host
    self.local_port = local_port
    self.priority   = 10
    self.master     = master
    
    self.confirmed_master = False
    self.client_started   = False
    self.ready            = False
    
    self.client_list      = client_list
    
    self.env = db.DBEnv()

    
  def open(self,local_path):
    # make sure local_path exists first?
    self.env.open(local_path, db.DB_CREATE | db.DB_INIT_TXN
            | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_LOCK |
            db.DB_INIT_REP | db.DB_RECOVER | db.DB_THREAD, 0666)
    
    self.env.repmgr_set_local_site( self.local_host, self.local_port )

    #print (self.local_host, self.local_port)
    #print self.client_list

    # add the clients
    for i in self.client_list:
      #print i
      self.env.repmgr_add_remote_site(i[0], i[1])
    
    # set number of replication sites
    self.env.rep_set_nsites( len(self.client_list) + 1 )
    #print "CLIENT LIST NUM IS:"
    #print len(self.client_list) + 1    


    if(self.master):
      self.env.rep_set_priority(self.priority)
    else:
      self.env.rep_set_priority(0)  # for now we want only one master  
    
    # set the ACK policy for transactions
    self.env.repmgr_set_ack_policy(db.DB_REPMGR_ACKS_ALL)

    # set up the notifier
    if(self.master):
      def confirmed_master(a,b,c) :
        if (b == db.DB_EVENT_REP_MASTER) or (b == db.DB_EVENT_REP_ELECTED) :
          self.confirmed_master = True
          self.ready = True
          #print "master started"
      self.env.set_event_notify(confirmed_master)
      self.env.repmgr_start(1, db.DB_REP_MASTER);
      
    else:
      def client_startupdone(a,b,c) :
        if b == db.DB_EVENT_REP_STARTUPDONE :
          self.client_started = True
          self.ready = True
      self.env.set_event_notify(client_startupdone)
      self.env.repmgr_start(1, db.DB_REP_CLIENT)
      #print "started as client"

  # so at the end of this we start the client
  #
  
  def wait_on_ready(self):
    "waits for it to be ready"
    if(self.ready):
      #print "deemed ready"
      return True
      
    timeout = time.time() + 3
    while( ( not self.client_started or not self.confirmed_master ) and not self.ready and timeout > time.time() ):
      time.sleep(0.2)
    #print self.client_started
    #print self.confirmed_master
    #print self.ready
    self.ready = True
    #print "deemed ready -- by default"
    
  def destroy(self):
    self.env.close()    

  def getFlags(self):
    if(self.master):
      return db.DB_CREATE | db.DB_THREAD
    else:
      #return db.DB_CREATE | db.DB_THREAD
      #return db.DB_CREATE
      #return db.DB_RDONLY
      return db.DB_THREAD | db.DB_RDONLY
      #return  db.DB_CREATE | db.DB_RDONLY


class BDB_Simple:
  def __init__(self):
    self.env = db.DBEnv()
    self.ready = False
    self.master = True
 
  def open(self, local_path):
    self.env.open(local_path, db.DB_CREATE | db.DB_INIT_TXN
            | db.DB_INIT_LOG | db.DB_INIT_MPOOL | db.DB_INIT_LOCK |
              db.DB_RECOVER | db.DB_THREAD)

  
  def wait_on_ready(self):
    if(self.ready):
      return True
    self.ready= True
  
  def destroy(self):
    self.env.close()  

  def getFlags(self):
    return db.DB_CREATE | db.DB_THREAD
    

class SimpleLogDB:
  def __init__(self, driver = BDB_Simple() ):
    self.driver = driver
    self.xyzDB = None
    self.data  = None
    # setup some variables?
  
  def open(self,local_path ):
    self.driver.open(local_path)
   
    # probably should wait on ready here 
    self.driver.wait_on_ready()
 
    flags = self.driver.getFlags()

    #print "PRE OPENING LOGS" + local_path
    self.data = db.DB(self.driver.env)
    if(not self.driver.master):
      while True :
          txn=self.driver.env.txn_begin()
          try :
              self.data.open("logs", db.DB_RECNO, flags , 0666, txn=txn)
              
          except db.DBRepHandleDeadError :
              txn.abort()
              self.data.close()
              self.data = db.DB(self.driver.env)
              continue

          txn.commit()
          break
    else:
      # not master
      txn=self.driver.env.txn_begin()
      self.data.open("logs", db.DB_RECNO, flags , 0666, txn=txn)
      txn.commit()

    # ok setup the rest of the stuff:
    
    #print "POST LOGS" + local_path
    
    txn=self.driver.env.txn_begin()
    # setup the secondary DB: time
    self.timeDB = db.DB(self.driver.env)
    self.timeDB.set_flags(db.DB_DUPSORT)
    self.timeDB.set_bt_compare(self.floatCompare)
    self.timeDB.open("time_index", db.DB_BTREE, flags, txn=txn)
    txn.commit()
        
    # setup the secondary DB: userDB
    txn=self.driver.env.txn_begin()
    self.userDB = db.DB(self.driver.env)
    self.userDB.set_bt_compare(self.intCompare)    
    self.userDB.set_flags(db.DB_DUPSORT)
    self.userDB.open("user_index", db.DB_BTREE, flags, txn=txn)
    txn.commit()

    # setup the secondary DB: locationDB
    txn=self.driver.env.txn_begin()
    self.xyzDB = db.DB(self.driver.env)
    self.xyzDB.set_flags(db.DB_DUPSORT)
    self.xyzDB.open("xyz_index", db.DB_BTREE, flags, txn=txn)
    txn.commit()

    # setup the secondary DB: deviceDB
    txn=self.driver.env.txn_begin()
    self.deviceDB = db.DB(self.driver.env)
    self.deviceDB.set_flags(db.DB_DUPSORT)
    self.deviceDB.open("device_index", db.DB_BTREE, flags, txn=txn)

    # commit the creation of the DBs
    txn.commit()

    # associate the tables.
    txn=self.driver.env.txn_begin()

    self.data.associate(self.xyzDB,  self.getXYZ,  db.DB_CREATE, txn=txn)
    self.data.associate(self.timeDB, self.getTime, db.DB_CREATE, txn=txn)
    self.data.associate(self.userDB, self.getUser, db.DB_CREATE, txn=txn)
    self.data.associate(self.deviceDB, self.getDevice, db.DB_CREATE, txn=txn)
    
    # I probably need the following queries:
    #  time-range
    # user, time-range
    # device, time-range
    # 
    # I also need to do some benchmarks to figure out if this is actually useful.
    
    #self.data.associate(self.devicetimeDB, self.getDevice, db.DB_CREATE, txn=txn)
    txn.commit()

  
  def close(self):
   
    try: 
      if(self.data is not None):
        self.data.close()
        self.data = None
      # close secondary indexes
      if( self.xyzDB is not None):
        self.xyzDB.close()
        self.xyzDB = None
        self.timeDB.close()
        self.userDB.close()
        self.deviceDB.close()
      
      # shut down the driver
      self.driver.destroy()
    except Exception, e:
      #print "GOT EXCEPTION"
      print e  
  #
  # Accessor Functions
  #
  def append(self, item ):
    self.driver.wait_on_ready()
    
    if(item.__class__ != str ): # probably not needed
      item = pickle.dumps(item)   # since we should always get a string
   
    #print "pre commit" 
    txn   = self.driver.env.txn_begin()
    recno = self.data.append(item)
    txn.commit()
    
    return recno
  
  def get(self, record):
    " get's an item by record number (unpickled) "
    return pickle.loads( self.data.get(record, None) ) 
  
  def get_by_timestamp(self, timestamp):
    "Returns a cursor at the current timestamp "
    c = self.timeDB.cursor()
    c.set_range(timestamp)
    return c

  def get_by_location(self, location):
    "Returns a cursor at the current location "
    c = self.xyzDB.cursor()
    c.set(location)
    return c

  def get_by_user(self, user):
    "Returns a cursor at the current user "
    c = self.userDB.cursor()
    c.set(user)
    return c

  def get_by_device(self, device):
    "Returns a cursor at the current device "
    c = self.deviceDB.cursor()
    c.set(device)
    return c

  
  
  
#
# Secondary Key Utility functions
#

  def getKey(self, data, key):
    #
    # If these were BYTE operations they'd be much faster
    #

    #print "#{key} - #{data}"
    q = pickle.loads(data)
    ret = q.get(key, None) 
    if(ret):
      print "RET " + str(ret)
      return str(ret)
    return db.DB_DONOTINDEX
    
  def getTime(self, priKey, priData):
    return self.getKey(priData, 'timestamp')
 
  def getUser(self, priKey, priData):
    return self.getKey(priData, 'user')

  def getDevice(self, priKey, priData):
    return self.getKey(priData, 'device') 

  def getXYZ(self, priKey, priData):
    print "getzxt"
    q = pickle.loads(priData)
    if(q.get('x', None) and q.get('y', None) and q.get('z', None) ):
      key = str(q['x']) + "|" + str(q['y']) + "|" + str(q['z'])
      return key
    else:
       return db.DB_DONOTINDEX
#
# Comparison Functions
# 
# these are really slow 
  
  def floatCompare(self, key1, key2 ):
    "Compare two floats -- pretty slow "
    # I guess this probably does really slow it down
    # because we'd do float compares a lot!
    print "floats"
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

  def intCompare(self, key1, key2 ):
    print "ints"
    "Compare two ints"
    print key1
    print key2 
    if(key1 ==''):
      key1 = 0
    if(key2 == ''):
      key2 = 0
    ret = int(key1) - int(key2)
    print ret
    return ret

