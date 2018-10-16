#!/usr/bin/env python3
import socket
import sys
import time
import configparser
import syslog
from libby import mysqldose
from libby.logger import logger 
import threading
from threading import Thread
import json
import urllib
import urllib.request



configfile = '/home/heizung/collectdata/collectdata.ini'
#logging = False
logging = True
logdata = True

#def logger(msg, logging=True):
#    if logging == True:
#        print(msg)
#        syslog.syslog(str(msg))


class kollektor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        logger("Starting Kollektorthread as " + threading.currentThread().getName(), logging)
        self.t_stop = threading.Event()
        self.delay = .1

        #self.parameters = ["Terrasse"]
        self.read_config()

        #self.db = mysqlcollect(self.mysqluser, self.mysqlpass, self.mysqlserv, self.mysqldb)
        self.db = mysqldose.mysqldose(self.mysqluser, self.mysqlpass, self.mysqlserv, self.mysqldb)

        
        #self.mysql_success = False
        self.db.start()


        logger("Starting UDP-Server at " + self.basehost + ":" + str(self.baseport), logging)
        self.e_udp_sock = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM ) 
        self.e_udp_sock.bind( (self.basehost,self.baseport) ) 
        
        
        #threading.Thread(target=self.threadwatcher).start()
        #print(threading.enumerate())

        threading.Thread(target=self.collect_oekofen).start()

    def collect_oekofen(self):
        #try:
            logger("Starting collection of Oekofen data thread as " + threading.currentThread().getName(), logging)
            while(not self.t_stop.is_set()):
                try:
                    with urllib.request.urlopen(self.pelle) as response:
                        mydata = response.read()
                        d = json.loads(mydata.decode())
                        now = time.strftime('%Y-%m-%d %H:%M:%S')
                        self.db.write(now, "OekoAussenTemp", float(d["system"]["L_ambient"])/10)
                        logger("OekoAussentemp = {} °C".format(float(d["system"]["L_ambient"])/10), logdata)
                        self.db.write(now, "OekoPumpeWestDrehzahl", float(d["sk1"]["L_pump"]))
                        logger("OekoPumpeWestDrehzahl = {} %".format(float(d["sk1"]["L_pump"])), logdata)
                        self.db.write(now, "OekoPumpeOstDrehzahl", float(d["sk3"]["L_pump"]))
                        logger("OekoPumpeOstDrehzahl = {} %".format(float(d["sk3"]["L_pump"])), logdata)
                        self.db.write(now, "OekoKollWestTemp", float(d["sk1"]["L_koll_temp"])/10)
                        logger("OekoKollWestTemp = {} °C".format(str(float(d["sk1"]["L_koll_temp"])/10)), logdata)
                        self.db.write(now, "OekoKollOstTemp", float(d["sk3"]["L_koll_temp"])/10)
                        logger("OekoKollOstTemp = {} °C".format(str(float(d["sk3"]["L_koll_temp"])/10)), logdata)
                        self.db.write(now, "OekoKollVorlaufTemp", float(d["se2"]["L_flow_temp"])/10)
                        logger("OekoKollVorlaufTemp = {} °C".format(str(float(d["se2"]["L_flow_temp"])/10)), logdata)
                        self.db.write(now, "OekoKollRuecklaufTemp", float(d["se2"]["L_ret_temp"])/10)
                        logger("OekoKollRuecklaufTemp = {} °C".format(str(float(d["se2"]["L_ret_temp"])/10)), logdata)
                        self.db.write(now, "OekoKollRuecklaufDurchfluss", float(d["se2"]["L_flow"])/100)
                        logger("OekoKollRuecklaufDurchfluss = {} l/min".format(str(float(d["se2"]["L_flow"])/100)), logdata)
                        self.db.write(now, "OekoKollLeistung", float(d["se2"]["L_pwr"])/10)
                        logger("OekoKollLeistung = {} kW".format(str(float(d["se2"]["L_pwr"])/10)), logdata)
                        self.db.write(now, "OekoSpUntenTemp", float(d["sk3"]["L_spu"])/10)
                        logger("OekoSpUntenTemp = {} °C".format(str(float(d["sk3"]["L_spu"])/10)), logdata)
                        self.db.write(now, "OekoSpMitteTemp", float(d["pu1"]["L_tpm_act"])/10)
                        logger("OekoSpMitteTemp = {} °C".format(str(float(d["pu1"]["L_tpm_act"])/10)), logdata)
                        self.db.write(now, "OekoSpObenTemp", float(d["pu1"]["L_tpo_act"])/10)
                        logger("OekoSpObenTemp = {} °C".format(str(float(d["pu1"]["L_tpo_act"])/10)), logdata)
                        self.db.write(now, "OekoFeuerraumTemp", float(d["pe1"]["L_frt_temp_act"])/10)
                        logger("OekoFeuerraumTemp = {} °C".format(str(float(d["pe1"]["L_frt_temp_act"])/10)), logdata)
                        self.db.write(now, "OekoKesselTemp", float(d["pe1"]["L_temp_act"])/10)
                        logger("OekoKesselTemp = {} °C".format(str(float(d["pe1"]["L_temp_act"])/10)), logdata)
                        self.db.write(now, "OekoPeStatus", float(d["pe1"]["L_state"]))
                        logger("OekoPeStatus = {}".format(str(float(d["pe1"]["L_state"]))), logdata)
                        self.db.write(now, "OekoPuPumpeDrehzahl", float(d["pu1"]["L_pump"]))
                        logger("OekoPuPumpeDrehzahl = {} %".format(str(float(d["pu1"]["L_pump"]))), logdata)
                        if d["circ1"]["L_pummp"] == "true":
                            self.db.write(now, "OekoCiStatus", 1)
                            logger("OekoCiStatus = 1", logdata)
                        else:
                            self.db.write(now, "OekoCiStatus", 0)
                            logger("OekoCiStatus = 0", logdata)
                        self.db.write(now, "OekoCiRetTemp", float(d["circ1"]["L_ret_temp"])/10)
                        logger("OekoCiRetTemp = {}".format(str(float(d["circ1"]["L_ret_temp"])/10)), logdata)
                        self.db.write(now, "OekoHkVlTempSet", float(d["hk1"]["L_flowtemp_set"])/10)
                        logger("OekoHkVlTempSet = {} °C".format(str(float(d["hk1"]["L_flowtemp_set"])/10)), logdata)
                        self.db.write(now, "OekoHkVlTempAct", float(d["hk1"]["L_flowtemp_act"])/10)
                        logger("OekoHkVlTempAct = {} °C".format(str(float(d["hk1"]["L_flowtemp_act"])/10)), logdata)
                        #print(now)
                except Exception as e:
                    logger("JSON error! "+str(e), logging)
                self.t_stop.wait(20)

            if self.t_stop.is_set():
                logger("Ausgeloggt", logging)
        #except Exception as e:
            #logger(e)



    def threadwatcher(self):
        try:
            logger("Starting Threadwatcherthread as " + threading.currentThread().getName(), logging)
            while(not self.t_stop.is_set()):
                enum = threading.enumerate()
                for i in range(len(enum)):
                    logger("[Threadwatcher]: " + str(enum[i]), logging)
                self.t_stop.wait(10)
            if self.t_stop.is_set():
                logger("Ausgewatcht", logging)
        except Exception as e:
            logger(str(e),logging)


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
            #print(self.pelle)
            print(self.parameter)

        except:
            logger("Configuration error", logging)

    


    def stop(self):
        self.t_stop.set()
        self.db.close()
        logger("Kollektor: So long sucker!", logging)
        #print(threading.enumerate())
        exit()


    def run(self):
        while True:
            try:
                data, addr = self.e_udp_sock.recvfrom( 1024 )# Puffer-Groesse ist 1024 Bytes. 
                #print(addr)
                #print(data.decode('utf-8')) 
                msg = data.decode('utf-8')
                msg_spl = msg.split(",")
                if (msg_spl[0]+msg_spl[1] in self.parameter):
                    logger(msg_spl[0]+" "+msg_spl[1]+ " " + msg_spl[2] + " " + msg_spl[3], logging)
                    answer = 'Sensor OK'
                    now = time.strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        self.db.write(now, msg_spl[0]+msg_spl[1], msg_spl[2])
                    except:
                        logger("Error writing to database", logging)
                else:
                    answer = 'Wrong Message'
                self.e_udp_sock.sendto(answer.encode('utf-8'), addr)
            except KeyboardInterrupt: # CTRL+C exit
                logger("So long sucker!", logging)
                #self.t_stop.set()
                self.stop()
                break





if __name__ == "__main__":
    kollektor = kollektor()
    kollektor.start()
    #kollektor.stop()
    #print(threading.enumerate())


