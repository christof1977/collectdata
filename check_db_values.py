#!/usr/bin/env python3

import time
from datetime import datetime
from datetime import timedelta
import os
from libby import mysqldose
import configparser
import logging

jsonfile = "conf_check.json"
configfile = "collectdata.ini"
realpath = os.path.realpath(__file__)
basepath = os.path.split(realpath)[0]
jsonfile = os.path.join(basepath, jsonfile)
configfile = os.path.join(basepath, configfile)

#params = {"LeahTemp":90, "FelixTemp":90, "VerbrauchHeizungEg":86400,
#        "VerbrauchHeizungDG":86400, "Furz":90, "OekoAussenTemp":90}

params = {"LeahTemp":90, "FelixTemp":90, "VerbrauchHeizungEg":86400,
        "VerbrauchHeizungDG":86400, "OekoAussenTemp":90,
        "tempFlur":90}
def main():
    try:
        config = configparser.ConfigParser()
        config.read(configfile)
        basehost = config['BASE']['Host']
        baseport = int(config['BASE']['Port'])
        mysqluser = config['BASE']['Mysqluser']
        mysqlpass = config['BASE']['Mysqlpass']
        mysqlserv = config['BASE']['Mysqlserv']
        mysqldb = config['BASE']['Mysqldb']
        pelle = config['BASE']['Pelle']
    except Exception as e:
        logging.error("Configuration error", str(e))


    db = mysqldose.Mysqldose(mysqluser, mysqlpass, mysqlserv, mysqldb)
    result = {}
    for key in params:
        now = datetime.now()
        now = now - timedelta(seconds = params[key])
        result[key] = db.read_one(key, dt=now)
    err = False
    output = ""
    for key in result:
        if result[key] == "Error":
            err = True
        output = output + key + ": " + str(result[key]) + "; "
    print(output)
    if(err):
        exit(1)
    else:
        exit(0)




if __name__ == "__main__":
    main()
