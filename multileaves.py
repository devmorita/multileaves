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
from firebase import firebase
import requests
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

logging.basicConfig(level=logging.DEBUG, format='[%(asctime)s][%(threadName)-10s]%(message)s',datefmt="%Y-%m-%d %H:%M:%S")


LeafeeAddrs = ['xx:xx:xx:xx:xx:xx']

ThreadType = {'main':'0', 'db':'1', 'firebase':'2', 'sensor':'9'}
ThreadData = {
        'main':{'type':ThreadType['main'], 'worker':None, 'queue':Queue(), 'heartbeat':0},
        'db':{'type':ThreadType['db'], 'worker':None, 'heartbeat':0},
        'firebase':{'type':ThreadType['firebase'], 'worker':None, 'heartbeat':0},
        'sensor':[]  # [{'type':'9', 'worker':WorkerThreadObject, 'peripheral':PeripheralObject}, {...}, {...}]
}

class DbSaveData():
    def __init__(self, sensorNo, sensorAddr, isClose, method):
        self.sensorNo   = sensorNo
        self.sensorAddr = sensorAddr
        self.isClose    = isClose
        self.method     = method

class QueueMessage():
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
            os.chmod(self.dbpath, 0777)

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
            conn.close()
            os.chmod(self.dbpath + '/' + self.dbname, 0666)
        else:
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
        global ThreadData
        logging.debug('[%-12s] with args(%s) and kwargs(%s)', 'startup', self.args, self.kwargs)

        while True:
            msg = self.queueBuf.get()
            if msg is None:
                loggin.debug("thread None")
            else:
                if (msg.cmd == 'insert' or msg.cmd == 'response'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd, json.dumps(msg.data.__dict__))
                   self.insertdb(msg.data.sensorNo , msg.data.isClose, msg.data.method)
                elif(msg.cmd == 'heartbeat'):
                   # logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   ThreadData['db']['heartbeat'] += 1;
                   # logging.debug('[%-12s] heartbeat = %s', 'heartbeat update', ThreadData['db']['heartbeat'])

                elif(msg.cmd == 'exit'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   break

                # DbWorkerThreadQueue.task_done()

class FirebaseThread(threading.Thread):

    queueBuf = Queue()

    url = 'https://xxxx.firebaseio.com'
    path = '/leafeed/dev/status/sensors/'
    fb = None

    #constructor
    def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        self.fb = firebase.FirebaseApplication(self.url, None)

    def putQueue(self, msg):
        self.queueBuf.put(msg)

    # db insert
    # method : 0 = notification, 1 = read in startup, 2 = read by heartbeat
    def saveStatus(self, addr, isclose):

        sensor = str(addr).replace(':', '')
        data = {"isclose": str(isclose), "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}

        try:
            # curlの場合は、112233445566.jsonが必要だが、このライブラリはいらない。
            result = self.fb.put('', self.path + '/' + sensor, data)
            logging.debug('[%-12s] sensor = %s, result = %s', 'put firebase', sensor, result)
            return True
        except (requests.HTTPError, Exception) as e:
            logging.debug('[%-12s] sensor = %s, result = %s', 'put firebase', sensor, -1)
            return False 

        logging.debug('[%-12s] sensor = %s, result = %s', 'put firebase', sensor, result)


    def readStatus(self, addr):

        sensor = str(addr).replace(':', '')
        # curlの場合は、112233445566.jsonが必要だが、このライブラリはいらない。
        result = self.fb.get(self.path + '/' + sensor, None)
        logging.debug('[%-12s] sensor = %s, result = %s', 'read firebase', sensor, result)

    def run(self):
        logging.debug('[%-12s] with args(%s) and kwargs(%s)', 'startup', self.args, self.kwargs)

        while True:
            msg = self.queueBuf.get()
            if msg is None:
                loggin.debug("thread None")
            else:
                if (msg.cmd == 'insert' or msg.cmd == 'response'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd, json.dumps(msg.data.__dict__))

                   if self.saveStatus(msg.data.sensorAddr, msg.data.isClose) == False:
                       global ThreadData
                       reqmsg = QueueMessage(ThreadData['firebase']['type'], '0', 'request')
                       ThreadData['sensor'][msg.fromThreadNo]['worker'].putQueue(reqmsg)
                       logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue send', reqmsg.fromThreadType, reqmsg.fromThreadNo, reqmsg.cmd)

                elif(msg.cmd == 'heartbeat'):
                   # logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   ThreadData['firebase']['heartbeat'] += 1;
                   # logging.debug('[%-12s] heartbeat = %s', 'heartbeat update', ThreadData['firebase']['heartbeat'])

                elif(msg.cmd == 'exit'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   break

                # DbWorkerThreadQueue.task_done()

class FirebaseAdminThread(threading.Thread):

    queueBuf = Queue()

    url = 'https://xxxx.firebaseio.com'
    path = '/leafeed/dev/status/sensors'
    cred = '/home/xxx/xxxx/xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx.json'

    app = Empty
    db = Empty
    isReady = False


    #constructor
    def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs

        if self.initApp() == True:
            self.initDb()

    def initApp(self):
        if self.app is not Empty:
            firebase_admin.delete_app(self.app)

        try:
            cred = credentials.Certificate(self.cred)
            self.app = firebase_admin.initialize_app(cred, {
                'databaseURL':self.url,
                'databaseAuthVariableOverride': {
                    'uid': 'my-service-worker'
                }
            })
            logging.debug('[%-12s] result = %s', 'init firebase', 0)
            return True

        except (requests.HTTPError, ValueError, Exception) as e:
            logging.debug('[%-12s] result = %s, message=%s', 'init firebase', -1, e.message)
            return False

    def initDb(self):
        if self.app is not Empty:
            try:
                self.db = db.reference(self.path, self.app)
                logging.debug('[%-12s] result = %s', 'init firebase db', 0)
                return True
            except (ValueError, Exception) as e:
                logging.debug('[%-12s] result = %s, message=%s', 'init firebase db', -1, e.message)
                return False
        else:
            return False

    def putQueue(self, msg):
        self.queueBuf.put(msg)

    # db insert
    # method : 0 = notification, 1 = read in startup, 2 = read by heartbeat
    def saveStatus(self, addr, isclose):

        sensor = str(addr).replace(':', '')
        if self.app is Empty or self.db is Empty:
            logging.debug('[%-12s] sensor = %s, result = %s', 'put firebase', sensor, -1)
            return False

        data = {"isclose": str(isclose), "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}

        try:
            self.db.child(sensor).set(data)
            logging.debug('[%-12s] sensor = %s, result = %s', 'put firebase', sensor, 0)
            return True
        except (requests.HTTPError, ValueError, TypeError, firebase_admin.db.ApiCallError, AssertionError) as e:
            logging.debug('[%-12s] sensor = %s, result = %s, msg = %s', 'put firebase', sensor, -2, e.message)
            return False 



    def readStatus(self, addr):

        sensor = str(addr).replace(':', '')
        # curlの場合は、112233445566.jsonが必要だが、このライブラリはいらない。
        result = self.db.child(sensor).get()
        logging.debug('[%-12s] sensor = %s, result = %s', 'read firebase', sensor, result)

    def run(self):
        logging.debug('[%-12s] with args(%s) and kwargs(%s)', 'startup', self.args, self.kwargs)

        while True:
            msg = self.queueBuf.get()
            if msg is None:
                loggin.debug("thread None")
            else:
                if (msg.cmd == 'insert' or msg.cmd == 'response'):
                   logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd, json.dumps(msg.data.__dict__))


                   '''
                   if self.app is Empty:
                       if self.initApp() is True:
                           self.initDb()
                   else:
                       if self.db is Empty:
                           self.initDb()
                   '''

                   isSuccess = False
                   if self.db is not Empty:
                       isSuccess = self.saveStatus(msg.data.sensorAddr, msg.data.isClose)

                   if isSuccess is False:
                       if self.initApp() == True:
                            self.initDb()

                       # send the request to the sender
                       global ThreadData
                       reqmsg = QueueMessage(ThreadData['firebase']['type'], '0', 'request')
                       ThreadData['sensor'][msg.fromThreadNo]['worker'].putQueue(reqmsg)
                       logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue send', reqmsg.fromThreadType, reqmsg.fromThreadNo, reqmsg.cmd)

                elif(msg.cmd == 'heartbeat'):
                   # logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                   ThreadData['firebase']['heartbeat'] += 1;
                   # logging.debug('[%-12s] heartbeat = %s', 'heartbeat update', ThreadData['firebase']['heartbeat'])

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
        ThreadData['sensor'][self.indexTh]['worker'].setLastStatus(isclose)
        ThreadData['sensor'][self.indexTh]['worker'].sendDbQueue(isclose, 1, 'insert')
        ThreadData['sensor'][self.indexTh]['worker'].sendFbQueue(isclose, 1, 'insert')


# worker thread class
class SensorThread(threading.Thread):

    indexTh = 0
    addr = None
    devid = None
    devindex = 0
    queueBuf = Queue()
    lastIsClose = 0

    #constructor
    def __init__(self, group=None, target=None, name=None,args=(), kwargs=None, verbose=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        self.indexTh = args[0]
        if ('devid' in self.kwargs):
            self.devid = kwargs['devid']
            self.devindex = int(self.devid) - 1
        if ('addr' in self.kwargs):
            self.addr = kwargs['addr']
        return

    def setLastStatus(self, isclose):
        self.lastIsClose = isclose

    def putQueue(self, msg):
        self.queueBuf.put(msg)

    def sendDbQueue(self, isclose, method, cmd):
        global ThreadData
        dbdata = DbSaveData(self.kwargs['devid'], self.kwargs['addr'], isclose, method)
        msg = QueueMessage(ThreadData['sensor'][self.indexTh]['type'], self.indexTh, cmd, dbdata)
        ThreadData['db']['worker'].putQueue(msg)
        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue send', msg.fromThreadType, msg.fromThreadNo, msg.cmd, json.dumps(msg.data.__dict__))

    def sendFbQueue(self, isclose, method, cmd):
        global ThreadData
        dbdata = DbSaveData(self.kwargs['devid'], self.kwargs['addr'], isclose, method)
        msg = QueueMessage(ThreadData['sensor'][self.indexTh]['type'], self.indexTh, cmd, dbdata)
        ThreadData['firebase']['worker'].putQueue(msg)
        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s, data = %s', 'queue send', msg.fromThreadType, msg.fromThreadNo, msg.cmd, json.dumps(msg.data.__dict__))

    def run(self):
        logging.debug('[%-12s] with args(%s) and kwargs(%s)', 'startup', self.args, self.kwargs)

        #check args
        if (self.devid is None):
            return 1
        if (self.addr is None):
            return 2

        global ThreadData

        '''
        while True:
            try:
                p = Peripheral(self.addr)
                logging.debug('[%-12s] to addr(%s)', 'connected', self.addr)

                p.setDelegate(NotificationDelegate(self.indexTh))
                ThreadData['sensor'][self.indexTh]['peripheral'] = p

                # read the current status
                data = p.readCharacteristic(0x002a)
                isclose = int(binascii.b2a_hex(data))
                self.sendDbQueue(isclose, 0, 'insert')

                # start notification
                p.writeCharacteristic(0x002b, "\x01\x00", True)

                break

            except btle.BTLEException:
                logging.debug('[%-12s] to addr(%s)', 'cannot connect', self.addr)
                time.sleep(1)
        '''


        p = Empty
        while True:
            try:
                if p is Empty:
                    raise btle.BTLEException(btle.BTLEException.DISCONNECTED, 'BeforeConnect')

                # receive notification
                p.waitForNotifications(1.0)

            # except btle.BTLEException as e:
            except (btle.BTLEException, Exception) as e:
                if e.message is not 'BeforeConnect':
                    logging.debug('[%-12s] to addr(%s)', 'disconnected', self.addr)

                # connection
                try:
                    p = Peripheral(self.addr)

                    # read the current status
                    data = p.readCharacteristic(0x002a)
                    isclose = int(binascii.b2a_hex(data))
                    self.lastIsClose = isclose
                    self.sendDbQueue(isclose, 0, 'insert')
                    self.sendFbQueue(isclose, 0, 'insert')

                    # start notification
                    p.setDelegate(NotificationDelegate(self.indexTh))
                    p.writeCharacteristic(0x002b, "\x01\x00", True)

                    ThreadData['sensor'][self.indexTh]['peripheral'] = p
                    logging.debug('[%-12s] to addr(%s)', 'connected', self.addr)

                except (btle.BTLEException, Exception) as e:
                    logging.debug('[%-12s] to addr(%s) msg=%s', 'cannot connect', self.addr, e.message)
                    time.sleep(1)


            # check message from other thread
            if not self.queueBuf.empty():
                msg = self.queueBuf.get()
                if msg is None:
                    loggin.debug("thread None")
                else:
                    if(msg.cmd == 'request'):
                        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                        global ThreadType
                        if (msg.fromThreadType == ThreadType['db']):
                            self.sendDbQueue(self.lastIsClose, 3, 'response')
                        elif (msg.fromThreadType == ThreadType['firebase']):
                            self.sendFbQueue(self.lastIsClose, 3, 'response')

                    elif(msg.cmd == 'reget'):
                        logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)
                        # read the current status
                        if p is not Empty:
                            data = p.readCharacteristic(0x002a)
                            isclose = int(binascii.b2a_hex(data))
                            self.lastIsClose = isclose
                            self.sendDbQueue(isclose, 0, 'insert')
                            self.sendFbQueue(isclose, 0, 'insert')

                    elif(msg.cmd == 'heartbeat'):
                       ThreadData['sensor'][self.devindex]['heartbeat'] += 1;
                       # logging.debug('[%-12s] heartbeat = %s', 'heartbeat update', ThreadData['sensor'][self.devindex]['heartbeat'])

                    elif(msg.cmd == 'exit'):
                        break

            # logging.debug('[%-12s]', 'None')


def sigterm_handler(signum, frame):
    logging.debug('[%-12s] signum = %s', 'signal', signum)

    global ThreadData

    # exit message
    msg = QueueMessage(ThreadData['main']['type'], 0, 'exit')

    # exit message to db thread
    ThreadData['db']['worker'].putQueue(msg)

    # exit message to firebase thread
    ThreadData['firebase']['worker'].putQueue(msg)

    # exit message to sensor threads
    for thSensor in ThreadData['sensor']:
        thSensor['worker'].putQueue(msg)

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
    msg = QueueMessage(ThreadData['main']['type'], 0, 'insert', dbdata)
    ThreadData['db']['worker'].putQueue(msg)
    dbdata = DbSaveData(9,8,7,6)
    msg = QueueMessage(ThreadData['main']['type'], 0, 'insert', dbdata)
    ThreadData['db']['worker'].putQueue(msg)
    '''

    # db thread start
    ThreadData['firebase']['worker'] = FirebaseAdminThread()
    ThreadData['firebase']['worker'].setDaemon(True)
    ThreadData['firebase']['worker'].start()


    # sensor thread start
    for i in range(len(LeafeeAddrs)):
        t = SensorThread(args=(i,), kwargs={'devid':str(i + 1), 'addr':LeafeeAddrs[i]})
        ThreadData['sensor'].append({'type':ThreadType['sensor'], 'worker':t, 'peripheral':Empty, 'heartbeat':0})

    for t in ThreadData['sensor']:
        t['worker'].setDaemon(True)
        t['worker'].start()


    heartbeat = 0;
    isSendOrCheck = True
    # recieve queue and send heartbeat
    while True:
        try:
            # msg = MainQueue.get()
            msg = ThreadData['main']['queue'].get(True, 1)
            logging.debug('[%-12s] from(type) = %s, from(no) = %s, cmd = %-10s', 'queue rcvd', msg.fromThreadType, msg.fromThreadNo, msg.cmd)

            # add procedure here
            if(msg.cmd == 'heartbeat'):
                logging.debug('recv heartbeat process')

        except Empty:
            # logging.debug('[%-12s]', 'None')

            # heartbeat message
            heartbeat += 1
            if (heartbeat > 5):
                heartbeat = 0
                if isSendOrCheck == True:
                    msg = QueueMessage(ThreadData['main']['type'], 0, 'heartbeat')
                    ThreadData['db']['worker'].putQueue(msg)
                    ThreadData['firebase']['worker'].putQueue(msg)
                    for thSensor in ThreadData['sensor']:
                        thSensor['worker'].putQueue(msg)
                else:
                    if ThreadData['db']['heartbeat'] <= 0:
                        logging.debug('[%-12s] type=%s', 'heartbeat error', 'db')
                        # restart thread

                    ThreadData['db']['heartbeat'] = 0

                    if ThreadData['firebase']['heartbeat'] <= 0:
                        logging.debug('[%-12s] type=%s', 'heartbeat error', 'firebase')
                        # restart thread

                    ThreadData['firebase']['heartbeat'] = 0

                    for thSensor in ThreadData['sensor']:
                        if thSensor['heartbeat'] <= 0:
                            logging.debug('[%-12s] type=%s', 'heartbeat error', 'sensor')
                            # restart thread

                        thSensor['heartbeat'] = 0

                # toggle send or check
                isSendOrCheck = not isSendOrCheck


            pass


if __name__ == "__main__":
    main(sys.argv)


