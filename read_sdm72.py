#!/usr/bin/env python3
import sys
sys.path.insert(0, './sdm_modbus/src')
sys.path.insert(0, './sdm_modbus')
import argparse
import json
import sdm_modbus
import time
import logging
import threading
from threading import Thread
from libby import udp_broadcast


server = "dose.home"
udpBcPort =  6664
logger = logging.getLogger('Stromzaehler')
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)


class readSdm72(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        self.sensor_values = {}
        self.sensor_values["EgStromLeistung"] = {}
        self.sensor_values["EgStromLeistung"]["Unit"] = "W"
        self.sensor_values["EgStromLeistung"]["Type"] = "Power"
        self.sensor_values["EgStromLeistung"]["Floor"] = "EG"
        self.sensor_values["EgStromEnergie"] = {}
        self.sensor_values["EgStromEnergie"]["Unit"] = "kWh"
        self.sensor_values["EgStromEnergie"]["Type"] = "Energy"
        self.sensor_values["EgStromEnergie"]["Floor"] = "EG"
        self.sensor_values["AllgStromLeistung"] = {}
        self.sensor_values["AllgStromLeistung"]["Unit"] = "W"
        self.sensor_values["AllgStromLeistung"]["Type"] = "Power"
        self.sensor_values["AllgStromLeistung"]["Floor"] = "Allg"
        self.sensor_values["AllgStromEnergie"] = {}
        self.sensor_values["AllgStromEnergie"]["Unit"] = "kWh"
        self.sensor_values["AllgStromEnergie"]["Type"] = "Energy"
        self.sensor_values["AllgStromEnergie"]["Floor"] = "Allg"
        self.sensor_values["OgStromLeistung"] = {}
        self.sensor_values["OgStromLeistung"]["Unit"] = "W"
        self.sensor_values["OgStromLeistung"]["Type"] = "Power"
        self.sensor_values["OgStromLeistung"]["Floor"] = "Og"
        self.sensor_values["OgStromEnergie"] = {}
        self.sensor_values["OgStromEnergie"]["Unit"] = "kWh"
        self.sensor_values["OgStromEnergie"]["Type"] = "Energy"
        self.sensor_values["OgStromEnergie"]["Floor"] = "EG"

        self.device = "/dev/ttyAMA0"
        self.stopbits = 1
        self.parity = "N"
        self.baud = 1200
        self.timeout = 2
        self.unit_eg = 1
        self.unit_allg = 2
        self.unit_og = 3
        
        self.meter_eg = sdm_modbus.SDM72(
            device=self.device,
            stopbits=self.stopbits,
            parity=self.parity,
            baud=self.baud,
            timeout=self.timeout,
            unit=self.unit_eg
        )
        
        self.meter_allg = sdm_modbus.SDM72(parent=self.meter_eg, unit=self.unit_allg)
        self.meter_og = sdm_modbus.SDM72(parent=self.meter_eg, unit=self.unit_og)


        self.udp = udp_broadcast.udpBroadcast() 
        self.broadcast_value()
        self.run()

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
        cnt = 1
        while(not self.bcastTstop.is_set()):
            try:
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                self.sensor_values["EgStromLeistung"]["Value"] = round(self.meter_eg.read("total_system_power"), 3)
                self.sensor_values["EgStromLeistung"]["Timestamp"] = now
                self.sensor_values["EgStromEnergie"]["Value"] = round(self.meter_eg.read("total_import_kwh"), 3)
                self.sensor_values["EgStromEnergie"]["Timestamp"] = now
                self.sensor_values["AllgStromLeistung"]["Value"] = round(self.meter_allg.read("total_system_power"), 3)
                self.sensor_values["AllgStromLeistung"]["Timestamp"] = now
                self.sensor_values["AllgStromEnergie"]["Value"] = round(self.meter_allg.read("total_import_kwh"), 3)
                self.sensor_values["AllgStromEnergie"]["Timestamp"] = now
                self.sensor_values["OgStromLeistung"]["Value"] = round(self.meter_og.read("total_system_power"), 3)
                self.sensor_values["OgStromLeistung"]["Timestamp"] = now
                self.sensor_values["OgStromEnergie"]["Value"] = round(self.meter_og.read("total_import_kwh"), 3)
                self.sensor_values["OgStromEnergie"]["Timestamp"] = now

                #print("{a:2.3f}W | {b:2.3f}kWh || {c:2.3f}W | {d:2.3f}kWh || {e:2.3f}W | {f:2.3f}kWh".format(a=p_eg, b=e_eg, c=p_allg, d=e_allg, e=p_og, f=e_og))
                if(cnt == 20):
                    store = 1
                    cnt = 1
                else:
                    store = 0
                    cnt += 1
                    logger.info(cnt)
                for sensor in self.sensor_values:
                    message = {"measurement":{sensor:{"Value":0,"Floor":"","Type":"Power","Unit":"W","Timestamp":"","Store":store}}}
                    message["measurement"][sensor]["Floor"] = self.sensor_values[sensor]["Floor"]
                    message["measurement"][sensor]["Type"] = self.sensor_values[sensor]["Type"]
                    message["measurement"][sensor]["Unit"] = self.sensor_values[sensor]["Unit"]
                    message["measurement"][sensor]["Value"] = self.sensor_values[sensor]["Value"]
                    message["measurement"][sensor]["Timestamp"] = self.sensor_values[sensor]["Timestamp"]
                    self.udp.send(message)
            except Exception as e:
                logger.error("Error in broadcast_value")
                logger.error(str(e))
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





