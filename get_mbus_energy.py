#!/usr/bin/env python3


import time
import os
import json
import logging
from libby import mbus
from libby import mysqldose
import configparser

configfile = 'collectdata.ini'
logging.basicConfig(level=logging.INFO)
    
if __name__ == '__main__':
    path = os.path.dirname(os.path.realpath(__file__))
    configfile = os.path.join(path, configfile)

    try:
        config = configparser.ConfigParser()
        config.read(configfile)
        mysqluser = config['BASE']['Mysqluser']
        mysqlpass = config['BASE']['Mysqlpass']
        mysqlserv = config['BASE']['Mysqlserv']
        mysqldb = config['BASE']['Mysqldb']

    except Exception as e:
        logging.error("Configuration error", str(e))

    mb = mbus.mbus()
    db = mysqldose.Mysqldose(mysqluser, mysqlpass, mysqlserv, mysqldb)

    result = mb.do_char_dev()
    energy = []

    job = json.loads(result)
    for i in (job['body']['records']):
        if i['type'] == 'VIFUnit.ENERGY_WH':
            energy.append(float(i['value']))
        if i['type'] == 'VIFUnit.FLOW_TEMPERATURE':
            flow_temp = float(i['value'])
        if i['type'] == 'VIFUnit.RETURN_TEMPERATURE':
            return_temp =float(i['value'])
        if i['type'] == 'VIFUnit.POWER_W' and i['function'] == 'FunctionType.INSTANTANEOUS_VALUE':
            power = float(i['value'])
        if i['type'] == 'VIFUnit.VOLUME_FLOW' and i['function'] == 'FunctionType.INSTANTANEOUS_VALUE':
            flow = float(i['value'])
    data =[]
    data.append("Verbrauch: " + str(max(energy)/1e6) + " MWh")
    data.append("Vorlauf: " + str(flow_temp) + "°C")
    data.append("Rücklauf: " + str(return_temp) + "°C")
    data.append("Leistung: " + str(power) + " W")
    data.append("Durchfluss: " + str(round(flow*1000,2)) + " l/h")

    db.write("now", "VerbrauchHeizungEg", max(energy))
    logging.info("Energieverbrauch: " + str(max(energy)/1e6) + "MWh")

