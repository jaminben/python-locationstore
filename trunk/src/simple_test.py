from bdb_interface import BDB_Replicated, BDB_Simple, SimpleLogDB


REMOTE_HOST = "127.0.0.1"
LOCAL_HOST  = "127.0.0.1"
LOCAL_PORT  = 9000
REMOTE_PORT = 9001

br = BDB_Replicated( LOCAL_HOST, LOCAL_PORT, True, 10, [ [REMOTE_HOST,REMOTE_PORT] ] )

br.open("test_dir1")

d   = db.DB( br.env )

txn = br.env.txn_begin()
d.open("logs", db.DB_RECNO, db.DB_CREATE , 0666, txn=txn)
txn.commit()


br1 = BDB_Replicated( LOCAL_HOST, LOCAL_PORT, True, 10, [ [REMOTE_HOST,REMOTE_PORT] ] )

br1.open("test_dir1")

d   = db.DB( br1.env )

txn = br1.env.txn_begin()
d.open("logs", db.DB_RECNO, db.DB_CREATE , 0666, txn=txn)
txn.commit()

