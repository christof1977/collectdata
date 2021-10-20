#!/usr/bin/env python3
import sys
sys.path.insert(0, './sdm_modbus/src')
sys.path.insert(0, './sdm_modbus')
import argparse
import json
import sdm_modbus
import time
import datetime
import logging
import threading
from threading import Thread
from libby import udp_broadcast
import socket


server = "dose.home"
udpBcPort =  6664
udp_port = 5009
logging.debug("")
logger = logging.getLogger(name='Stromzaehler')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


class readSdm72(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        self.sensor_values = {}
        self.sensor_values["EgElPwrL1"] = {}
        self.sensor_values["EgElPwrL1"]["Unit"] = "W"
        self.sensor_values["EgElPwrL1"]["Type"] = "Power"
        self.sensor_values["EgElPwrL1"]["Command"] = "p1_power_active"
        self.sensor_values["EgElPwrL1"]["Floor"] = "EG"

        self.sensor_values["EgElPwrL2"] = {}
        self.sensor_values["EgElPwrL2"]["Unit"] = "W"
        self.sensor_values["EgElPwrL2"]["Type"] = "Power"
        self.sensor_values["EgElPwrL2"]["Command"] = "p2_power_active"
        self.sensor_values["EgElPwrL2"]["Floor"] = "EG"

        self.sensor_values["EgElPwrL3"] = {}
        self.sensor_values["EgElPwrL3"]["Unit"] = "W"
        self.sensor_values["EgElPwrL3"]["Type"] = "Power"
        self.sensor_values["EgElPwrL3"]["Command"] = "p3_power_active"
        self.sensor_values["EgElPwrL3"]["Floor"] = "EG"

        self.sensor_values["EgElEnTot"] = {}
        self.sensor_values["EgElEnTot"]["Unit"] = "kWh"
        self.sensor_values["EgElEnTot"]["Type"] = "Energy"
        self.sensor_values["EgElEnTot"]["Command"] = "total_import_kwh"
        self.sensor_values["EgElEnTot"]["Floor"] = "EG"


        self.sensor_values["AllgElPwrL1"] = {}
        self.sensor_values["AllgElPwrL1"]["Unit"] = "W"
        self.sensor_values["AllgElPwrL1"]["Type"] = "Power"
        self.sensor_values["AllgElPwrL1"]["Command"] = "p1_power_active"
        self.sensor_values["AllgElPwrL1"]["Floor"] = "Allg"

        self.sensor_values["AllgElPwrL2"] = {}
        self.sensor_values["AllgElPwrL2"]["Unit"] = "W"
        self.sensor_values["AllgElPwrL2"]["Type"] = "Power"
        self.sensor_values["AllgElPwrL2"]["Command"] = "p2_power_active"
        self.sensor_values["AllgElPwrL2"]["Floor"] = "Allg"

        self.sensor_values["AllgElPwrL3"] = {}
        self.sensor_values["AllgElPwrL3"]["Unit"] = "W"
        self.sensor_values["AllgElPwrL3"]["Type"] = "Power"
        self.sensor_values["AllgElPwrL3"]["Command"] = "p3_power_active"
        self.sensor_values["AllgElPwrL3"]["Floor"] = "Allg"

        self.sensor_values["AllgElEnTot"] = {}
        self.sensor_values["AllgElEnTot"]["Unit"] = "kWh"
        self.sensor_values["AllgElEnTot"]["Type"] = "Energy"
        self.sensor_values["AllgElEnTot"]["Command"] = "total_import_kwh"
        self.sensor_values["AllgElEnTot"]["Floor"] = "Allg"


        self.sensor_values["OgElEnTot"] = {}
        self.sensor_values["OgElEnTot"]["Unit"] = "kWh"
        self.sensor_values["OgElEnTot"]["Type"] = "Energy"
        self.sensor_values["OgElEnTot"]["Command"] = "total_import_kwh"
        self.sensor_values["OgElEnTot"]["Floor"] = "Og"


        self.device = "/dev/ttyAMA0"
        self.stopbits = 1
        self.parity = "N"
        self.baud = 1200
        self.timeout = 2

        self.floors = {"Eg":1, "Og":3, "Allg":2}

        self.meter = {}
        self.meter["Eg"] = sdm_modbus.SDM72(
            device=self.device,
            stopbits=self.stopbits,
            parity=self.parity,
            baud=self.baud,
            timeout=self.timeout,
            unit=self.floors["Eg"]
        )

        self.meter["Allg"] = sdm_modbus.SDM72(parent=self.meter["Eg"], unit=self.floors["Allg"])
        self.meter["Og"] = sdm_modbus.SDM72(parent=self.meter["Eg"], unit=self.floors["Og"])

        self.hostname = socket.gethostname()
        self.basehost = self.hostname + ".home"
        self.t_stop = threading.Event()
        self.udpServer()
        self.udp = udp_broadcast.udpBroadcast()
        self.broadcast_value()
        self.run()

    def udpServer(self):
        logger.info("Starting UDP-Server at " + self.basehost + ":" + str(udp_port))
        self.udpSock = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
        self.udpSock.bind( (self.basehost,udp_port) )

        udpT = threading.Thread(target=self._udpServer)
        udpT.setDaemon(True)
        udpT.start()

    def _udpServer(self):
        logger.info("Server laaft")
        while(not self.t_stop.is_set()):
            try:
                data, addr = self.udpSock.recvfrom( 1024 )# Puffer-Groesse ist 1024 Bytes.
                #logger.debug("Kimm ja scho")
                ret = self.parseCmd(data) # Abfrage der Fernbedienung (UDP-Server), der Rest passiert per Interrupt/Event
                self.udpSock.sendto(str(ret).encode('utf-8'), addr)
            except Exception as e:
                try:
                    self.udpSock.sendto(str('{"answer":"error"}').encode('utf-8'), addr)
                    logger.warning("Uiui, beim UDP senden/empfangen hat's kracht!" + str(e))
                except Exception as o:
                    logger.warning("Uiui, beim UDP senden/empfangen hat's richtig kracht!" + str(o))

    def parseCmd(self, data):
        data = data.decode()
        try:
            jcmd = json.loads(data)
            logger.debug(jcmd)
        except:
            logger.warning("Das ist mal kein JSON, pff!")
            ret = json.dumps({"answer": "Kaa JSON Dings!"})
            return(ret)
        try:
            if(jcmd['command'] == "getImportPower"):
                ret = self.get_import_power(jcmd)
            elif(jcmd['command'] == "getFloors"):
                ret = self.get_floors()
            elif(jcmd['command'] == "getAlive"):
                ret = self.get_alive()
        except Exception as e:
            logging.error(e)
            ret = json.dumps({"answer":"Error","Value":"Not a valid command"})
        return(ret)

    def get_alive(self):
        """ function to see, if we are alive
        """
        return(json.dumps({"name":self.hostname,"answer":"Freilich"}))

    def get_floors(self):
        logger.info(list(self.floors.keys()))
        return json.dumps({"Floors":list(self.floors.keys())})

    def get_import_power(self, jcmd):
        '''
        This functions reads some values from the electrical power counter and retuns them as json string.
        '''
        if(jcmd["device"] == "current"):
            data = {}
            data["Data"] = {}
            if(jcmd["floor"] in ["Eg", "Og", "Allg"]):
                logger.info("Reading values from SDM72, floor {}".format(jcmd["floor"]))
                value = round(self.meter[jcmd["floor"]].read("total_import_kwh"), 3)
                data["Data"]["Energy"] = {"Value":value, "Unit": "kWh"}
                data["Data"]["Floor"] = jcmd["floor"]
                data["Data"]["Device"] = "SDM72"
                data["Data"]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                data = {"answer":"Error","Value":"Not a valid floor"}
        else:
            data = {"answer":"Error","Value":"Not a valid device: {}".format(jcmd["device"])}
        return json.dumps(data)

    def broadcast_value(self):
        '''
        Starts the UDP sensor broadcasting daemon thread
        '''
        self.bcastTstop = threading.Event()
        bcastT = threading.Thread(target=self._broadcast_value)
        bcastT.setDaemon(True)
        bcastT.start()

    def _broadcast_value(self):
        '''
        This function is running as a thread and performs an UDP
        broadcast of sensor values every 20 seconds on port udpBcPort.
        This datagram could be fetched by multiple clients for purposes
        of display or storage.
        '''
        logger.info("Starting UDP Sensor Broadcasting Thread" + threading.currentThread().getName())
        ts_null = datetime.datetime.now()
        while(not self.bcastTstop.is_set()):
            try:
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                ts = datetime.datetime.now()
                #self.sensor_values["EgStromLeistung"]["Value"] = round(self.meter_eg.read("total_system_power"), 3)
                #self.sensor_values["EgStromLeistung"]["Timestamp"] = now
                #self.sensor_values["EgStromEnergie"]["Value"] = round(self.meter_eg.read("total_import_kwh"), 3)
                #self.sensor_values["EgStromEnergie"]["Timestamp"] = now
                #self.sensor_values["AllgStromLeistung"]["Value"] = round(self.meter_allg.read("total_system_power"), 3)
                #self.sensor_values["AllgStromLeistung"]["Timestamp"] = now
                #self.sensor_values["AllgStromEnergie"]["Value"] = round(self.meter_allg.read("total_import_kwh"), 3)
                #self.sensor_values["AllgStromEnergie"]["Timestamp"] = now
                #self.sensor_values["OgStromLeistung"]["Value"] = round(self.meter_og.read("total_system_power"), 3)
                #self.sensor_values["OgStromLeistung"]["Timestamp"] = now
                #self.sensor_values["OgStromEnergie"]["Value"] = round(self.meter_og.read("total_import_kwh"), 3)
                #self.sensor_values["OgStromEnergie"]["Timestamp"] = now

                #print("{a:2.3f}W | {b:2.3f}kWh || {c:2.3f}W | {d:2.3f}kWh || {e:2.3f}W | {f:2.3f}kWh".format(a=p_eg, b=e_eg, c=p_allg, d=e_allg, e=p_og, f=e_og))
                if((ts - ts_null).total_seconds() > 18):
                    store = 1
                    #print(ts)
                    ts_null = datetime.datetime.now()
                else:
                    store = 0
                for sensor in self.sensor_values:
                    message = {"measurement":{sensor:{"Value":0,"Floor":"","Type":"Power","Unit":"W","Timestamp":"","Store":store}}}
                    message["measurement"][sensor]["Floor"] = self.sensor_values[sensor]["Floor"]
                    message["measurement"][sensor]["Type"] = self.sensor_values[sensor]["Type"]
                    message["measurement"][sensor]["Unit"] = self.sensor_values[sensor]["Unit"]
                    message["measurement"][sensor]["Value"] = self.sensor_values[sensor]["Value"]
                    message["measurement"][sensor]["Timestamp"] = self.sensor_values[sensor]["Timestamp"]
                    self.udp.send(message)
            except Exception as e:
                pass
                #logger.error("Error in broadcast_value")
                #logger.error(str(e))
            self.bcastTstop.wait(1)

    def run(self):
        while True:
            try:
                time.sleep(.5)
            except KeyboardInterrupt: # CTRL+C exit
                self.stop()
                break

    def stop(self):
        self.bcastTstop.set()
        logger.info("So long sucker!")
        sys.exit()
        return

if __name__ == "__main__":
    read_sdm72 = readSdm72()
    read_sdm72.start()





