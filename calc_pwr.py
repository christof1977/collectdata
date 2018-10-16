#!/usr/bin/env python3
import socket
import sys
import time
from datetime import datetime
import syslog
from libby import mysqldose
from libby.logger import logger
import numpy as np

logging = True


def get_pwr_by_hour(date):
    db = mysqldose.mysqldose("heizung", "heizung", "dose.fritz.box", "heizung")
    db.start()

    tot_m = 0
    for d in range (1,31):
        date = "2018-10-"+str(d).zfill(2)
        tot_d = 0
        for h in range(0,24):
            result = db.calc_pwr_h(date + " " + str(h).zfill(2) + ":%")
            #print(h, ": ", result)

            if(result != None):
                tot_d = tot_d + result
        print("Gesamt (",date,"): ",round(tot_d,1), " kWh")

        tot_m =  tot_m + tot_d


    print()
    print("Gesamt Monat: ", round(tot_m,1), "kWh")


    db.close()


def get_pwr_by_day(date):
    db = mysqldose.mysqldose("heizung", "heizung", "dose.fritz.box", "heizung")
    db.start()

    values, datetimes = db.read_many("OekoKollLeistung", date+"%")
    i = 0
    x = []
    for val in datetimes:
        if(i == 0):
            distance = (datetimes[i] - datetime.strptime(date+" 00:00:00", "%Y-%m-%d %H:%M:%S"))
            x.append(distance.total_seconds())
        else:
            distance = (datetimes[i]-datetimes[i-1])
            x.append(x[i-1]+distance.total_seconds())
        i+=1

    pwr = np.trapz(values,x)
    return pwr/3600

    db.close()


if __name__ == "__main__":
    #main()
    tot_m = 0
    for d in range (1,32):
        date = "2018-10-"+str(d).zfill(2)
        #date = "2018-10-16"
        tot_d = round(get_pwr_by_day(date),3)
        print(date, ":", tot_d, "kWh")
        tot_m = tot_m +tot_d

    print("Gesamt:", tot_m, "kWh")


