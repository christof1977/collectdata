#!/usr/bin/env python3
import socket
import sys
import time
import configparser
import syslog
from libby import mysqldose
from libby.logger import logger 
#from mysqlcollect import mysqlcollect
import threading
from threading import Thread
import subprocess

configfile = '/home/heizung/collectdata/collectdata.ini'
logging = False
devices = ['sdb','sdc','sdd','sde']

#def logger(msg):
#    if logging == True:
#        print(msg)
#        syslog.syslog(str(msg))


class hddstatus(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        logger("Starting hddstatusthread as " + threading.currentThread().getName())
        self.t_stop = threading.Event()
        self.delay = .1

        self.read_config()
        #self.db = mysqlcollect("heizung", "heizung", "dose.fritz.box", "hddstatus")
        self.db = mysqldose.mysqldose(self.mysqluser, self.mysqlpass, self.mysqlserv, "hddstatus")
        
        self.db.start()

        threading.Thread(target=self.mainthread).start()

    def stop(self):
        self.t_stop.set()
        self.db.close()
        logger("hddstatus: So long sucker!")
        exit()

    def mainthread(self):
        while(not self.t_stop.is_set()):
            for device in devices:
                status = self.getstatus(device)
            self.t_stop.wait(60)


    def getstatus(self, device):
        result = subprocess.check_output(["hdparm", "-C" ,"/dev/"+device])
        #print("active: "+result.find("active"))
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        _result = result.decode()
        if(_result.find("active") != -1):
            logger(device + ": active")
            self.db.write(now, device, 1)
        if(_result.find("standby") != -1):
            logger(device + ": sleeping")
            self.db.write(now, device, 0)
        #print(result)
        return 1

    def read_config(self):
        try:
            self.config = configparser.ConfigParser()
            self.config.read(configfile)
            self.basehost = self.config['BASE']['Host']
            self.baseport = int(self.config['BASE']['Port'])
            self.mysqluser = self.config['BASE']['Mysqluser']
            self.mysqlpass = self.config['BASE']['Mysqlpass']
            self.mysqlserv = self.config['BASE']['Mysqlserv']
            self.mysqldb = self.config['BASE']['Mysqldb']

        except:
            logger("Configuration error", logging)

 




if __name__ == "__main__":
    hddstatus = hddstatus()
    hddstatus.start()
    #for device in devices:
    #    status = hddstatus.getstatus(device)
        #print(status)
    #hddstatus.stop()
    

