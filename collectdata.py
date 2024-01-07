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
from libby.remote import udpRemote
import threading
from threading import Thread
import json
import urllib
import urllib.request
import logging
import select
import schedule
import paho.mqtt.publish as publish


logger = logging.getLogger('Kollektor')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


log_interval = 20
oeko_interval = 20

jsonfile = "conf.json"
configfile = "collectdata.ini"
realpath = os.path.realpath(__file__)
basepath = os.path.split(realpath)[0]
jsonfile = os.path.join(basepath, jsonfile)
configfile = os.path.join(basepath, configfile)
print(jsonfile)

eth_addr = ''
udp_port = 6663
udpBcPort =  6664

class kollektor():
    def __init__(self):
        self.hostname = socket.gethostname()
        self.basehost = self.hostname
        data = self.read_json(jsonfile)
        self.conf_pelle = data.pop("pelle")
        self.read_config()
        self.db = mysqldose.Mysqldose(self.mysqluser, self.mysqlpass, self.mysqlserv, self.mysqldb)
        self.fetch_oekofendata()
        self.collect_oekofendata()
        self.broadcast_value()
        self.udpRx()
        self.udpServer()
        self.run()

    def read_json(self, jsonfile):
        with open(jsonfile, "r") as fhd:
            data = json.load(fhd)
        return (data)

    def write_value(self, timestamp, descr, value, unit, log=False, db=True):
        if(db):
            try:
                self.db.write(timestamp, descr, value)
            except Exception as e:
                logger.error("While writing to database in \
                    _collect_oekofendata:" + str(e))
        if(log):
            logger.info("{} = {} {}".format(descr, value, unit))

    def collect_oekofendata(self):
        self.codTstop = threading.Event()
        codT = threading.Thread(target=self._collect_oekofendata)
        codT.setDaemon(True)
        codT.start()

    def _collect_oekofendata(self):
        '''
        Collecting data from Oekofen Oven and store the into database
        '''
        logger.info("Starting collection of Oekofen data thread as " + threading.currentThread().getName())
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
                    publish.single("oekofen/{}/{}".format(level, name), value, hostname="mqtt.plattentoni.de", client_id="Oekofen")
            except Exception as e:
                logger.error("JSON error! "+str(e))
            self.codTstop.wait(log_interval)
        if self.codTstop.is_set():
            logger.info("Ausgeloggt")

    def fetch_oekofendata(self):
        self.fodTstop = threading.Event()
        fodT = threading.Thread(target=self._fetch_oekofendata)
        fodT.setDaemon(True)
        fodT.start()

    def _fetch_oekofendata(self):
        '''
        Getting data from Oekofen Oven via JSON interface.
        '''
        doit = True
        if(doit):
            while(not self.fodTstop.is_set()):
                try:
                    with urllib.request.urlopen(self.pelle) as response:
                        data = response.read()
                        self.oekofendata = json.loads(data.decode())
                except Exception as e:
                    logger.error(str(e))
                self.fodTstop.wait(oeko_interval)

    def get_oekofendata(self):
        '''
        This function returns the json string from the Oekofen device, which is
        stored within this program.
        '''
        try:
            logger.info("Delivering Oekofendata")
            return json.dumps(self.oekofendata)
        except:
            logger.error("An error occured while trying to deliver oekofendata")
            ans = {"answer": "An error occured while trying to deliver oekofendata"}
            return(json.dumps(ans))


    def get_umwaelzpumpe(self):
        '''
        This funcion returns the state of the Umwaelzpumpe of HK1.
        '''
        try:
            ans = {"answer": self.oekofendata["hk1"]["L_pump"]}
            return(json.dumps(ans))
        except:
            logger.error("An error occured while trying to deliver state of the famous umwaelzpumpe")
            ans = {"answer": "An error occured while trying to deliver state of the famous umwaelzpumpe"}
            return(json.dumps(ans))

    def broadcast_value(self):
        self.bcastTstop = threading.Event()
        bcastT = threading.Thread(target=self._broadcast_value)
        bcastT.setDaemon(True)
        bcastT.start()

    def _broadcast_value(self):
        udpSock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        udpSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT,1)
        udpSock.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST, 1)
        udpSock.settimeout(0.1)
        while(not self.bcastTstop.is_set()):
            try:
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                message = {"measurement":{"tempOekoAussen":{"Name":"","Floor":"EG","Value":0,"Type":"Temperature","Unit":"°C","Timestamp":"","Store":0}}}
                message["measurement"]["tempOekoAussen"]["Name"] = "Aussentemperatur Pelle"
                message["measurement"]["tempOekoAussen"]["Value"] = round(float(self.oekofendata["system"]["L_ambient"])/10,1)
                message["measurement"]["tempOekoAussen"]["Timestamp"] = now
                udpSock.sendto(json.dumps(message).encode(),("<broadcast>",udpBcPort))
            except Exception as e:
                logger.error(str(e))
            self.bcastTstop.wait(20)

    def udpServer(self):
        self.udpSeTstop = threading.Event()
        udpSeT = threading.Thread(target=self._udpServer)
        udpSeT.setDaemon(True)
        udpSeT.start()

    def _udpServer(self):
        udpSock = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
        udpSock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        udpSock.bind((eth_addr,udp_port))
        logger.info("Starting UDP Server %s:%s" % (eth_addr, udp_port))
        while(not self.udpSeTstop.is_set()):
            ready = select.select([udpSock], [], [], .1)
            if ready[0]:
                data, addr = udpSock.recvfrom(4096)
                try:
                    data = json.loads(data.decode())
                except:
                    logger.error("shit happens while decoding json string")
                logger.debug("{0} says {1}".format(addr, data))
                if("command" in data.keys()):
                    ret = self.parse_command(data)
                    udpSock.sendto(str(ret).encode('utf-8'),addr)

    def parse_command(self, data):
        if(data["command"] == "getOekofendata"):
            return self.get_oekofendata()
        elif(data["command"] == "getUmwaelzpumpe"):
            return self.get_umwaelzpumpe()
        elif(data["command"] == "getStoredValues"):
            return json.dumps(self.measurements)
        elif(data['command'] == "getAlive"):
            return self.get_alive()

    def get_alive(self):
        """ function to see, if we are alive
        """
        logging.debug("Alive and kicking!")
        return(json.dumps({"name":self.hostname,"answer":"Freilich"}))

    def udpRx(self):
        logger.debug("fuck")
        self.udpRxTstop = threading.Event()
        udpRxT = threading.Thread(target=self._udpRx)
        udpRxT.setDaemon(True)
        udpRxT.start()

    def _udpRx(self):
        logger.debug("Starting UDP client on port %s" % udpBcPort)
        udpclient = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, \
                socket.IPPROTO_UDP)  # UDP
        udpclient.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        udpclient.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        udpclient.bind(("", udpBcPort))
        udpclient.setblocking(0)

        self.measurements = {}

        while(not self.udpRxTstop.is_set()):
            ready = select.select([udpclient], [], [], .1)
            if ready[0]:
                data, addr = udpclient.recvfrom(8192)
                logger.debug(data.decode())
                try:
                    message = json.loads(data.decode())
                    if("measurement" in message.keys()):
                        meas = message["measurement"]
                        for key in meas:
                            value = float(meas[key]["Value"])
                            unit = meas[key]["Unit"]
                            timestamp = meas[key]["Timestamp"]
                            if(meas[key]["Store"] == 1):
                                db = True
                            else:
                                db = False
                            self.write_value(timestamp,
                                    key, value, unit, db=db)
                            self.measurements[key] = {"Value":value,
                                    "Unit":unit, "Timestamp":timestamp}
                except Exception as e:
                    logger.error(str(e))

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
            logger.error("Configuration error")

    def stop(self):
        self.t_stop.set()
        self.db.close()
        logger.info("Kollektor: So long sucker!")
        exit()

    def run(self):
        while True:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    Kollektor = kollektor()


