#!/usr/bin/env python3
import socket
import sys
import time
import syslog
from libby import mysqldose
from libby.logger import logger

logging = True


def main():
    db = mysqldose.mysqldose("heizung", "heizung", "dose.fritz.box", "heizung")
    db.start()

    tot_m = 0
    for d in range (1,31):
        date = "2018-09-"+str(d).zfill(2)
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





if __name__ == "__main__":
    main()


