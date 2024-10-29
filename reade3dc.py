#!/usr/bin/env python3

from e3dc import E3DC
import pprint
import json
from time import sleep, time
import paho.mqtt.client as mqtt
import logging
import logging.handlers

# create logger
if(__name__ == "__main__"):
    logging.basicConfig(level=logging.INFO)
    #logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger()
    handler = logging.handlers.SysLogHandler(address = '/dev/log')
    formatter = logging.Formatter('ReadE3DC: %(module)s: %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def get_item(nd, prefix=""):
    ''' Walk recursively through nested dict "nd" and return a flat dict
    '''
    ret = {}
    for key in nd:
        if isinstance(nd[key], dict):
            r =  get_item(nd[key], prefix=prefix+"/"+str(key))
            for k in r:
                ret[k] = r[k]
        else:
            k = prefix+"/"+str(key)
            k = k[1:]
            ret[k] = nd[key]
    return ret


if( __name__ == "__main__"):
    with open("e3dc_conf.json") as conffile:
            config = json.load(conffile)

    logger.info("Starting E3DC data collection")
    try:
        e3dc = E3DC(E3DC.CONNECT_LOCAL,
                    username = config["e3dc"]["user"],
                    password = config["e3dc"]["pass"],
                    ipAddress = config["e3dc"]["host"],
                    key = config["e3dc"]["key"],
                    configuration = config["e3dc"]["config"])
        logger.info("Established connection to S10")
    except:
        logger.error("Connection to S10 failed! Exiting.")
        exit()

    with open(config["e3dc"]["mapfile"]) as mapfile:
        mapping = json.load(mapfile)
        flat_map = get_item(mapping)

    try:
        mqttclient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, config["mqtt"]["client"])
        mqttclient.username_pw_set(config["mqtt"]["user"], config["mqtt"]["pass"])
        mqttclient.connect(config["mqtt"]["host"],
                           config["mqtt"]["port"],
                           config["mqtt"]["timeout"],
                )
        mqttclient.loop_start()
        logger.info("Connection to MQTT broker successful")
    except:
        logger.error("No connection to MQTT broker! Exiting.")
        exit()

    # The following connections are performed through the RSCP interface
    wait = config["interval"]["general"]
    logging.info("Starting data acquisition with interval of {} seconds".format(wait))
    while True:
        values = {}

        pollinger = get_item(e3dc.poll(keepAlive=True))
        for rec in pollinger:
            # Find MQTT topic for measured value in flat_map
            try:
                key = [k for k, v in flat_map.items() if v == rec][0]
                values[key] = pollinger[rec]
            except:
                pass

        pvi = get_item(e3dc.get_pvi_data(keepAlive=True))
        for rec in pvi:
            # Find MQTT topic for measured value in flat_map
            try:
                key = [k for k, v in flat_map.items() if v == rec][0]
                values[key] = pvi[rec]
            except:
                pass

        pm = get_item(e3dc.get_powermeter_data(6, keepAlive=True))
        for rec in pm:
            # Find MQTT topic for measured value in flat_map
            try:
                key = [k for k, v in flat_map.items() if v == rec][0]
                values[key] = pm[rec]
            except:
                pass

        bat_data  = e3dc.get_batteries_data(keepAlive=True)
        for list_item in bat_data: # list iteration required, because get_batteries_data returns list
            battery = get_item(list_item)
        for rec in battery:
            # Find MQTT topic for measured value in flat_map
            try:
                key = [k for k, v in flat_map.items() if v == rec][0]
                values[key] = battery[rec]
            except:
                pass
        sleep(2)

        for key in values:
            mqttclient.publish(key, values[key], retain=True)

    mqttclient.loop_stop()
    e3dc.disconnect()
