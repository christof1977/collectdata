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
import socket
import paho.mqtt.publish as publish

mqttbhost = "mqtt.plattentoni.de"
mqttbuser = "raspi"
mqttbpass = "parsi"

server = "dose"
udpBcPort =  6664
udp_port = 5009
logging.debug("")
logger = logging.getLogger(name='Stromzaehler')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)


class readSdm72(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        self.device = "/dev/ttyAMA0"
        self.stopbits = 1
        self.parity = "N"
        self.baud = 19200
        self.timeout = .25
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

        self.sendperm = 1

        self.meas = ["voltage", "current", "power_factor", "phase_angle", "power_active", "power_apparent", "power_reactive"]
        self.phases = {1:"l1", 2:"l2", 3:"l3"}

        self.hostname = socket.gethostname()
        self.basehost = ""
        self.t_stop = threading.Event()
        self.udpServer()
        self.broadcast_value()
        self.run()

    def udpServer(self):
        logger.info("Starting UDP-Server at " + self.basehost + ":" + str(udp_port))
        self.udpSock = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
        self.udpSock.bind( (self.basehost,udp_port) )

        udpT = threading.Thread(target = self._udpServer, daemon = True)
        #udpT.setDaemon(True)
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
            elif(jcmd['command'] == "getPhaseValue"):
                ret = self.get_phase_value_json(jcmd)
            elif(jcmd['command'] == "setSendperm"):
                ret = self.set_send_perm(jcmd)
        except Exception as e:
            logging.error(e)
            ret = json.dumps({"answer":"Error","Value":"Not a valid command"})
        return(ret)

    def set_send_perm(self, jcmd) ->int:
        """This function sets or unsets the variable self.sendperm.
        If set to "On", the values of the counters specified in the broadcast function are send permanently.
        If set to "Off", the values are only sent in the specified interval for saving.
        Returns the value of self.sendperm
        """
        try:
            if(jcmd["Value"] == "On"):
                self.sendperm = 1
            else:
                self.sendperm = 0
            return json.dumps({"Answer":"Success","Value":self.sendperm})
        except:
            return json.dumps({"Answer":"Error","Value":"Error during setting sendperm"})


    def get_alive(self):
        """ function to see, if we are alive
        """
        return(json.dumps({"name":self.hostname,"answer":"Freilich"}))

    def get_floors(self):
        #logger.info(list(self.floors.keys()))
        return json.dumps({"Floors":list(self.floors.keys())})
        
    def get_phase_value_json(self, jcmd):
        '''
        This function unpacks the json command received via the API and calls the function get_phase_value
        with the appropriate parameters. Finally, it returns the well formatted json string expected by
        the receiver.
        '''
        try:
            floor = jcmd["Floor"]
        except:
            return json.dumps({"Answer":"Error","Value":"No floor given"})
        if floor not in list(self.floors.keys()):
            return json.dumps({"Answer":"Error","Value":"Floor not existing. Must be in {}".format(list(self.floors.keys()))})
        try:
            phase = jcmd["Phase"]
        except:
            return json.dumps({"Answer":"Error","Value":"No phase given"})
        if phase not in [1, 2, 3]:
            return json.dumps({"Answer":"Error","Value":"Phase not existing. Must be in [1, 2, 3]"})
        try:
            meas = jcmd["Meas"]
        except:
            return json.dumps({"Answer":"Error","Value":"No measurement given"})
        if meas not in self.meas:
            return json.dumps({"Answer":"Error","Value":"Measurement not existing. Must be in {}".format(self.meas)})
        res = self.get_phase_value(floor=floor, phase=phase, meas=meas)

        data = {}
        data["Data"] = {}
        data["Data"][res["name"]] = {"Value":res["value"], "Unit": res["unit"]}
        data["Data"]["Floor"] = floor
        data["Data"]["Device"] = "SDM72"
        data["Data"]["Timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return json.dumps(data)

    def get_phase_value(self, floor=None, meas=None, phase=None):
        '''
        This function reads the desired value from the desired counter, reformats the output
        and returns the result as dict res = {"value":value, "unit":unit}.
        '''
        if phase not in self.phases.keys():
            return json.dumps({"answer":"Error","Value":"Not a valid phase (must be in [1, 2, 3]"})
        if meas not in self.meas:
            return json.dumps({"answer":"Error","Value":"Not a valid measurement"})
        cmd = self.phases[phase] + "_" + meas
        value = round(self.meter[floor].read(cmd), 3)
        unit = self.meter[floor].registers[cmd][6]
        name = self.meter[floor].registers[cmd][5]
        return {"name":name, "value":value, "unit":unit}

    def get_import_power(self, jcmd):
        '''
        This functions reads some values from the electrical power counter and retuns them as json string.
        '''
        if(jcmd["Device"] == "SDM72"):
            data = {}
            data["Data"] = {}
            if(jcmd["Floor"] in ["Eg", "Og", "Allg"]):
                floor = jcmd["Floor"]
                cmd = "import_energy_active"
                logger.info("Reading values from SDM72, floor {}".format(jcmd["Floor"]))
                value = round(self.meter[jcmd["Floor"]].read(cmd), 3)
                unit = self.meter[floor].registers[cmd][6]
                name = self.meter[floor].registers[cmd][5]
                data["Data"]["Energy"] = {"Value":value*1000, "Unit":"Wh"}
                data["Data"]["Floor"] = jcmd["Floor"]
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
        bcastT = threading.Thread(target = self._broadcast_value, daemon = True)
        #bcastT.setDaemon(True)
        bcastT.start()

    def _broadcast_value(self):
        '''
        This function is running as a thread and performs an UDP
        broadcast of sensor values every 20 seconds on port udpBcPort.
        This datagram could be fetched by multiple clients for purposes
        of display or storage.
        '''
        logger.info("Starting UDP Sensor Broadcasting Thread" + threading.current_thread().name)
        ts_null = datetime.datetime.now()
        while(not self.bcastTstop.is_set()):
            try:
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                ts = datetime.datetime.now()
                if((ts - ts_null).total_seconds() > 18):
                    store = 1
                    ts_null = datetime.datetime.now()
                else:
                    store = 0
                if(self.sendperm or store):
                    for fl in ["Eg", "Og", "Allg"]:
                        pwr = []
                        for phase in [1,2,3]:
                            res = self.get_phase_value(floor=fl, phase=phase, meas="power_active")
                            name = res["name"].replace("(","").replace(")","").split()
                            name="Power/"+fl+"/"+name[0].replace("P","")+"/"+name[1]+name[2]
                            #name = res["name"].replace(" ", "").replace("(", "").replace(")", "") # + fl
                            val = res["value"]
                            pwr.append(val)
                            unit = res["unit"]
                            typ = name
                            #publish.single("Power/"+fl+"/"+typ, val, hostname=mqttbhost, client_id="Stromzaehler",auth = {"username":mqttbuser, "password":mqttbpass})
                            publish.single(name, val, hostname=mqttbhost, client_id="Stromzaehler",auth = {"username":mqttbuser, "password":mqttbpass})
                        pwr_tot = round(sum(pwr),3)
                        publish.single("Power/"+fl+"/Sum/PowerActive", pwr_tot, hostname=mqttbhost, client_id="Stromzaehler",auth = {"username":mqttbuser, "password":mqttbpass})
            except Exception as e:
                logger.error("Error in broadcast_value")
                logger.error(str(e))
            self.bcastTstop.wait(3)

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





