#!/usr/bin/env python3

'''
JSON Format for measurement values
{
"measurement":{
  "tempFlur":{
     "Name":" Temperatur Flur",
     "Floor":"EG",
     "Type":"Temperature",
     "Value":"Einwert",
     "Unit":"°C",
     "Comment":"What's this?",
     "Store":1,
     "Timestamp":""
    }
  }
}
'''

import os
import socket
import sys
import time
import configparser
import syslog
from libby import mysqldose
import threading
from threading import Thread
import json
import urllib
import urllib.request
import logging
import select

logging.basicConfig(level=logging.INFO)

log_interval = 20
oeko_interval = 20

jsonfile = "conf.json"
configfile = "collectdata.ini"
realpath = os.path.realpath(__file__)
basepath = os.path.split(realpath)[0]
jsonfile = os.path.join(basepath, jsonfile)
configfile = os.path.join(basepath, configfile)
print(jsonfile)

class kollektor():
    def __init__(self):
        data = self.read_json(jsonfile)
        self.conf_pelle = data.pop("pelle")
        self.read_config()
        self.db = mysqldose.mysqldose(self.mysqluser, self.mysqlpass, self.mysqlserv, self.mysqldb)
        self.db.start()
        self.fetch_oekofendata()
        self.collect_oekofendata()
        self.broadcast_value()
        self.udpRx()
        self.run()

    def read_json(self, jsonfile):
        with open(jsonfile, "r") as fhd:
            data = json.load(fhd)
        return (data)

    def write_value(self, timestamp, descr, value, unit, log=True, db=False):
        if(db):
            self.db.write(timestamp, descr, value)
        if(log):
            logging.info("{} = {} {}".format(descr, value, unit))

    def collect_oekofendata(self):
        self.codTstop = threading.Event()
        codT = threading.Thread(target=self._collect_oekofendata)
        codT.setDaemon(True)
        codT.start()

    def _collect_oekofendata(self):
        '''
        Collecting data from Oekofen Oven and store the into database
        '''
        logging.info("Starting collection of Oekofen data thread as " + threading.currentThread().getName())
        while(not self.codTstop.is_set()):
            try:
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                d = self.oekofendata
                c = self.conf_pelle
                for key in c:
                    level = '{}'.format(c[key]["oe_level"])
                    name = '{}'.format(c[key]["oe_name"])
                    value =  d[level][name]
                    factor = float(c[key]["Factor"])
                    try:
                        value = float(value)
                        value =round(value * factor,1)
                    except:
                        pass
                    if(value == "true" or value == "True"):
                        value = 1
                    elif(value == "false" or value == "False"):
                        value = 0
                    unit = '{}'.format(c[key]["Unit"])
                    self.write_value(now, key, value, unit)
            except Exception as e:
                logging.info("JSON error! "+str(e))
            self.codTstop.wait(log_interval)
        if self.codTstop.is_set():
            logging.info("Ausgeloggt")

    def fetch_oekofendata(self):
        self.oekofendata = None
        self.fodTstop = threading.Event()
        fodT = threading.Thread(target=self.f_oekofendata)
        fodT.setDaemon(True)
        fodT.start()

    def f_oekofendata(self):
        while(not self.fodTstop.is_set()):
            try:
                with urllib.request.urlopen(self.pelle) as response:
                    data = response.read()
                    self.oekofendata = json.loads(data.decode())
            except Exception as e:
                logging.error(str(e))
            self.fodTstop.wait(oeko_interval)

    def get_oekofendata(self):
        '''
        This function returns the json string from the Oekofen device, which is
        stored within this program.
        '''
        return self.oekofendata

    def broadcast_value(self):
        self.bcastTstop = threading.Event()
        bcastT = threading.Thread(target=self._broadcast_value)
        bcastT.setDaemon(True)
        bcastT.start()

    def _broadcast_value(self):
        while(not self.bcastTstop.is_set()):
            self.bcastTstop.wait(1)

    def udpRx(self):
        self.udpRxTstop = threading.Event()
        udpRxT = threading.Thread(target=self._udpRx)
        udpRxT.setDaemon(True)
        udpRxT.start()

    def _udpRx(self):
        port =  44445
        logging.debug("Starting UDP client on port ", port)
        udpclient = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, \
                socket.IPPROTO_UDP)  # UDP
        udpclient.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        udpclient.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udpclient.bind(("", port))
        udpclient.setblocking(0)

        while(not self.udpRxTstop.is_set()):
            ready = select.select([udpclient], [], [], .1)
            if ready[0]:
                data, addr = udpclient.recvfrom(4096)
                try:
                    message = json.loads(data.decode())
                    if("measurement" in message.keys()):
                        meas = message["measurement"]
                        for key in meas:
                            self.write_value(meas[key]["Timestamp"],
                                    key, float(meas[key]["Value"]),
                                    meas[key]["Unit"])
                except Exception as e:
                    logging.error(str(e))

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
            self.parameter = self.config['BASE']['Parameter'].split(',')
            self.pelle = self.config['BASE']['Pelle']
            print(self.parameter)
        except:
            logging.error("Configuration error")

    def stop(self):
        self.t_stop.set()
        self.db.close()
        logging.info("Kollektor: So long sucker!")
        exit()

    def run(self):
        while True:
            time.sleep(.1)

    def _run(self):
        while True:
            try:
                data, addr = self.e_udp_sock.recvfrom( 1024 )# Puffer-Groesse ist 1024 Bytes. 
                msg = data.decode('utf-8')
                msg_spl = msg.split(",")
                if (msg_spl[0]+msg_spl[1] in self.parameter):
                    logging.info(msg_spl[0]+" "+msg_spl[1]+ " " + msg_spl[2] + " " + msg_spl[3])
                    answer = 'Sensor OK'
                    now = time.strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        self.write_value(now, msg_spl[0]+msg_spl[1], msg_spl[2])
                    except:
                        logging.error("Error writing to database")
                else:
                    answer = 'Wrong Message'
                self.e_udp_sock.sendto(answer.encode('utf-8'), addr)
            except KeyboardInterrupt: # CTRL+C exit
                logging.info("So long sucker!")
                #self.t_stop.set()
                self.stop()
                #break

if __name__ == "__main__":
    Kollektor = kollektor()


