# -*- coding: utf-8 -*-
"""
    Leafee OpenClose notification receiver program
"""

from bluepy.btle import Peripheral
import bluepy.btle as btle
import binascii
import sys
import sqlite3
import os
import logging
from multiprocessing import Queue
from Queue import Empty
import threading
import signal
import time
import json

logging.basicConfig(level=logging.DEBUG, format='(%(threadName)-10s) %(message)s',)


LeafeeAddrs = ['xx:xx:xx:xx:xx:xx']

ThreadType = {'main':'0', 'db':'1', 'sensor':'2'}
ThreadData = {
        'main':{'type':'0', 'worker':None, 'queue':Queue()},
        'db':{'type':'1', 'worker':None},
        'sensor':[]  # [{'type':'2', 'worker':WorkerThreadObject, 'peripheral':PeripheralObject}, {...}, {...}]
}

class DbSaveData():
    
    def __init__(self, sensorNo, sensorAddr, isClose, method):
        self.sensorNo   = sensorNo 
        self.sensorAddr = sensorAddr
        self.isClose    = isClose
        self.method     = method

class DbQueueMessage():
    def __init__(self, fromThreadType, fromThreadNo, cmd, data=None):
        self.fromThreadType = fromThreadType 
        self.fromThreadNo = fromThreadNo 
        self.cmd = cmd
        self.data = data 

# worker thread class
class DbThread(threading.Thread):

    queueBuf = Queue()

    # dbname = "/var/www/data/sqlite"
    dbpath = "/tmp/sqlite"
    dbname = "sensor.db"
    dbtable = "lf_log_table"

    #constructor
    def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs

        if not os.path.exists(self.dbpath):
            os.makedirs(self.dbpath)

        # connect sqlite: this creates new db, if not exists
        conn = sqlite3.connect(self.dbpath + '/' + self.dbname)
        c = conn.cursor()
 
        # check the target table exists or not 
        checkdb = conn.execute("SELECT * FROM sqlite_master WHERE type='table' and name='%s'" % self.dbtable)

        # create new table if not exists
        if checkdb.fetchone() == None:
            create_table = 'CREATE TABLE ' + self.dbtable + '(id INTEGER PRIMARY KEY AUTOINCREMENT, devid INTEGER, isclose INTEGER, method INTEGER DEFAULT 0, created TEXT DEFAULT CURRENT_TIMESTAMP)'
            c.execute(create_table)
            conn.commit()
 
        # disconnect
        conn.close()

    def putQueue(self, msg):
        self.queueBuf.put(msg)

    # db insert 
    # method : 0 = notification, 1 = read in startup, 2 = read by heartbeat
    def insertdb(self, devid, isclose, method = 0):

        data= (str(devid),str(isclose),str(method))

        # connect sqlite: this creates new db, if not exists
        conn = sqlite3.connect(self.dbpath + '/' + self.dbname)
        c = conn.cursor()
 
        # insert into data 
        sql = "INSERT INTO " + self.dbtable + " (devid,isclose,method) VALUES (?,?,?)"
        c.execute(sql, data)
        conn.commit()
 
        # disconnect
        conn.close()


    def run(self):
        logging.debug('[%-12s] with args(%s) and kwargs(%s)', 'startup', self.args, self.kwargs)

        while True:
            msg = self.queueBuf.get()
            if msg is None:
                loggin.debug("thread None")
            else:
                if (msg.cmd == 'insert'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd, json.dumps(msg.data.__dict__))
                   self.insertdb(msg.data.sensorNo , msg.data.isClose, msg.data.method)
                elif(msg.cmd == 'heartbeat'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   logging.debug('heartbeat process')
                elif(msg.cmd == 'exit'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   break

                # DbWorkerThreadQueue.task_done()

# Call Back at notification
class NotificationDelegate(btle.DefaultDelegate):

    indexTh = 0;

    def __init__(self, number):
        btle.DefaultDelegate.__init__(self)
        self.number = number 
        self.indexTh = number

    def handleNotification(self, cHandle, data): 
        isclose = int(binascii.b2a_hex(data))
        logging.debug('[%-12s] sensoer number = %s, value = %s', 'notification', self.number, isclose)

        global ThreadData
        ThreadData['sensor'][self.indexTh]['worker'].sendDbQueue(isclose, 1)


# worker thread class
class SensorThread(threading.Thread):

    indexTh = 0
    addr = None 
    devid = None 
    queueBuf = Queue()

    #constructor
    def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        self.indexTh = args[0]
        if ('devid' in self.kwargs):
            self.devid = kwargs['devid']
        if ('addr' in self.kwargs):
            self.addr = kwargs['addr']
        return

    def putQueue(self, msg):
        self.queueBuf.put(msg)

    def sendDbQueue(self, isclose, method):
        global ThreadData
        dbdata = DbSaveData(self.kwargs['devid'], self.kwargs['addr'], isclose, method)
        dbmsg = DbQueueMessage(ThreadData['sensor'][self.indexTh]['type'], self.indexTh, 'insert', dbdata)
        ThreadData['db']['worker'].putQueue(dbmsg)
        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue send', dbmsg.fromThreadType, dbmsg.fromThreadNo, dbmsg.cmd, json.dumps(dbmsg.data.__dict__))

    def run(self):
        logging.debug('[%-12s] with args(%s) and kwargs(%s)', 'startup', self.args, self.kwargs)

        #check args
        if (self.devid is None):
            return 1 
        if (self.addr is None):
            return 2 

        global ThreadData
        peripheral = ThreadData['sensor'][self.indexTh]['peripheral']
        peripheral.setDelegate(NotificationDelegate(self.indexTh))

        # read the current status
        data = peripheral.readCharacteristic(0x002a)
        isclose = int(binascii.b2a_hex(data))
        self.sendDbQueue(isclose, 0)


        # start notification 
        peripheral.writeCharacteristic(0x002b, "\x01\x00", True)

        while True:
            # receive notification
            peripheral.waitForNotifications(1.0)

            # check message from other thread
            if not self.queueBuf.empty():
                msg = self.queueBuf.get()
                if msg is None:
                    loggin.debug("thread None")
                else:
                    if(msg.cmd == 'heartbeat'):
                        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                        logging.debug('heartbeat process')
                    elif(msg.cmd == 'exit'):
                        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                        break


def sigterm_handler(signum, frame):
    logging.debug('[%-12s] signum = %s', 'signal', signum)

    global ThreadData 

    # exit message
    dbmsg = DbQueueMessage(ThreadData['main']['type'], 0, 'exit')

    # exit message to db thread
    ThreadData['db']['worker'].putQueue(dbmsg)

    # exit message to sensor threads
    for thSensor in ThreadData['sensor']:
        thSensor['worker'].putQueue(dbmsg)

    # sig alrm before exit
    signal.alarm(10)

def sigalrm_handler(signum, frame):
    logging.debug('[%-12s] signum = %s', 'signal', signum)
    exit(1)

def main(args):

    '''
    # signal
    # http://ja.pymotw.com/2/signal/
    signals_to_names = {}
    for n in dir(signal):
        if n.startswith('SIG') and not n.startswith('SIG_'):
            signals_to_names[getattr(signal, n)] = n

    for s, name in sorted(signals_to_names.items()):
        handler = signal.getsignal(s)
        if handler is signal.SIG_DFL:
            handler = 'SIG_DFL'
        elif handler is signal.SIG_IGN:
            handler = 'SIG_IGN'
        print '%-10s (%2d):' % (name, s), handler
    '''

    '''
    # ignore signal sigint
    signal.signal(signal.SIGINT, signal.SIG_IGN) 
    '''

    # set signal sigterm and sigalrm
    signal.signal(signal.SIGTERM, sigterm_handler)
    signal.signal(signal.SIGALRM, sigalrm_handler)

    # thread
    # http://ja.pymotw.com/2/threading/
    global ThreadType
    global ThreadData 

    # db thread start
    ThreadData['db']['worker'] = DbThread()
    ThreadData['db']['worker'].setDaemon(True)
    ThreadData['db']['worker'].start()

    '''
    # test
    dbdata = DbSaveData(1,2,3,4)
    dbmsg = DbQueueMessage(ThreadData['main']['type'], 0, 'insert', dbdata)
    ThreadData['db']['worker'].putQueue(dbmsg)
    dbdata = DbSaveData(9,8,7,6)
    dbmsg = DbQueueMessage(ThreadData['main']['type'], 0, 'insert', dbdata)
    ThreadData['db']['worker'].putQueue(dbmsg)
    '''

    global LeafeeAddrs

    for i in range(len(LeafeeAddrs)):
        t = SensorThread(args=(i,), kwargs={'devid':str(i + 1), 'addr':LeafeeAddrs[i]})
        ThreadData['sensor'].append({'type':ThreadType['sensor'], 'worker':t, 'peripheral':Peripheral(LeafeeAddrs[i])})

    for t in ThreadData['sensor']:
        t['worker'].setDaemon(True)
        t['worker'].start()


    # recieve queue
    while True:
        try:
            # msg = MainQueue.get()
            msg = ThreadData['main']['queue'].get(True, 1)
            logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
            logging.debug('add procedure here')
        except Empty:
            logging.debug('[%-12s]', 'None')



if __name__ == "__main__":
    main(sys.argv)
