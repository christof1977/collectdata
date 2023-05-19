#!/usr/bin/env python3

import os
import sys
import getopt
import syslog
import configparser
import time
import datetime
import numpy as np
from libby import mysqldose
from libby import datevalues
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

import logging
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

configfile = "collectdata.ini"
realpath = os.path.realpath(__file__)
basepath = os.path.split(realpath)[0]
configfile = os.path.join(basepath, configfile)


writeDB = True

class DailyReport(object):
    '''
    TODO
    '''
    def __init__(self):
        self.read_config()
        self.maria = mysqldose.Mysqldose(self.mysqluser, self.mysqlpass, self.mysqlserv, self.mysqldb)

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
            self.influxserv = self.config['BASE']['Influxserv']
            self.influxtoken = self.config['BASE']['Influxtoken']
            self.influxbucket = self.config['BASE']['Influxbucket']
            self.influxorg = self.config['BASE']['Influxorg']
        except Exception as e:
            logging.error("Configuration error")
            logging.error(e)

    def influx_query(self, parameter, fil=None, day=None):
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "'+parameter+'" and r["_field"] == "value")'
        if(fil in ["pos", "neg"]):
            if(fil=="pos"):
                query = query + '|> filter(fn: (r) => r["_value"] > 0.0)'
            else:
                query = query + '|> filter(fn: (r) => r["_value"] <= 0.0)'
        query = query + ' |> aggregateWindow( \
                            every: 1h, \
                            fn: (tables=<-, column) => \
                                tables \
                                    |> integral(unit: 1h) \
                                    |> map(fn: (r) => ({ r with _value: r._value / 1000.0}))) \
                      |> aggregateWindow(fn: sum, every: 1mo) '
        return query

    def influx_energy_query(self, parameter, fil=None, day=None):
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "'+parameter+'" and r["_field"] == "value")'
        if(fil in ["pos", "neg"]):
            if(fil=="pos"):
                query = query + '|> filter(fn: (r) => r["_value"] > 0.0)'
            else:
                query = query + '|> filter(fn: (r) => r["_value"] <= 0.0)'
        query = query + ' |> integral(unit: 1h) \
                          |> map(fn: (r) => ({ r with _value: r._value / 1000.0}))'
        return query

    def update_solar_gain(self, day=None):
        '''
        This function reads the solar gain of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        logging.info("Calculating solar gain and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        res = self.maria.read_day(start_date, "OekoKollLeistung")
        pwr = []
        for line in res:
            pwr.append(line[2])
        try:
            pwr = round(sum(pwr)/(3600/(86400/len(pwr))), 3)
        except ZeroDivisionError:
            pwr = 0
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        result = query_api.query(self.influx_energy_query("oekofen/se2/L_pwr", day=day))
        #for table in result:
        #    for record in table:
        #        print(record)
        logging.info("Solarertrag: {}kWh".format(pwr))
        if(writeDB):
            self.maria.write_day(start_date, "Solarertrag", pwr)
        else:
            logging.warning("Not writing to DB")
        return result

    def update_pellet_consumption(self, day=None):
        '''
        This function reads the pellet consumption of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        logging.info("Calculating pellet consumption and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        res = self.maria.read_day(start_date, "OekoStoragePopper")
        val = [0]
        for line in res:
            try:
                if(line[2] != val[-1]):
                    val.append(line[2])
            except:
                pass
        val.pop(0)
        val = np.array(val)
        diffval = np.diff(val)
        noval = len(diffval)
        diffval = diffval[diffval != -1]
        verbrauch = noval - len(diffval)
        logging.info("Solarertrag: {}kg".format(verbrauch))
        if(writeDB):
            self.maria.write_day(start_date, "VerbrauchPellets", verbrauch)
        else:
            logging.warning("Not writing to DB")


    def update_energy_consumption(self, parameter, day=None):
        '''
        This function reads the value of the power consumption of a
        given day or today and the value of the day before and calculates the
        power consuption of this day. The values is stored in the daily table.
        '''
        logging.info("Getting consumed energy ({}) of day and  writing value to daily table".format(parameter))
        start_date, end_date = datevalues.date_values(day)
        try:
            con_today  = self.maria.read_day(start_date, parameter)[0][2]
            logging.info("Consumed today: {}".format(con_today))
            yesterday = start_date - datetime.timedelta(1)
            con_yesterday  = self.maria.read_day(yesterday, parameter)[0][2]
            logging.info("Consumed yesterday: {}".format(con_yesterday))
            con = (con_today - con_yesterday)/1000
            logging.info("{}: {}kWh".format(parameter, con))
            if(writeDB):
                self.maria.write_day(start_date, parameter, con)
            else:
                logging.warning("Not writing to DB")
        except Exception as e:
            logging.error("Something went wrong: " + str(e))

    def update_water_consumption(self, parameter, day=None):
        '''
        This function reads the value of the water consumption of a
        given day or today and the value of the day before and calculates the
        water consuption of this day. The values is stored in the daily table.
        '''
        logging.info("Getting consumed energy ({}) of day and  writing value to daily table".format(parameter))
        start_date, end_date = datevalues.date_values(day)
        try:
            con_today  = self.maria.read_day(start_date, parameter)[0][2]
            logging.info("Consumed today: {}".format(con_today))
            yesterday = start_date - datetime.timedelta(1)
            con_yesterday  = self.maria.read_day(yesterday, parameter)[0][2]
            logging.info("Consumed yesterday: {}".format(con_yesterday))
            con = (con_today - con_yesterday)
            logging.info("{}: {}m^3".format(parameter, con))
            if(writeDB):
                self.maria.write_day(start_date, parameter, con)
            else:
                logging.warning("Not writing to DB")
        except Exception as e:
            logging.error("Something went wrong: " + str(e))
    def influx_calc_energy(self, day=None):
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        start_date, end_date = datevalues.date_values_influx(day)
        print(start_date)
        return
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "E3DC/EMS_DATA/EMS_POWER_HOME" and r["_field"] == "value")'
        result = query_api.query(query)
        for table in result:
            for record in table:
                print(record)

    def update_electrical(self, day=None):
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        parameter = {"VerbrauchStromHaus":{"par":"E3DC/EMS_DATA/EMS_POWER_HOME","filter":None},
                "PVErtragAc":{"par":"E3DC/EMS_DATA/EMS_POWER_PV","filter":None},
                "PVErtragDcOst":{"par":"E3DC/PVI_DATA/0/PVI_DC_POWER/0/PVI_VALUE","filter":None},
                "PVErtragDcWest":{"par":"E3DC/PVI_DATA/0/PVI_DC_POWER/1/PVI_VALUE","filter":None},
                "Netzbezug":{"par":"E3DC/EMS_DATA/EMS_POWER_GRID","filter":"pos"},
                "Netzeinspeisung":{"par":"E3DC/EMS_DATA/EMS_POWER_GRID","filter":"neg"},
                "Batterieladung":{"par":"E3DC/EMS_DATA/EMS_POWER_BAT","filter":"pos"},
                "Batterieentladung":{"par":"E3DC/EMS_DATA/EMS_POWER_BAT","filter":"neg"}}
        for key in parameter:
            result = query_api.query(self.influx_query(parameter[key]["par"], fil=parameter[key]["filter"] , day=day))
            for table in result:
                for record in table:
                    value = round(record.get_value(), 2)
            start_date, end_date = datevalues.date_values(day)
            logging.info("Calculating {} of day and writing value to daily table".format(key))
            logging.info("{}: {}kWh".format(key, value))
            if(writeDB):
                self.maria.write_day(start_date, key, value)
            else:
                logging.warning("Not writing to DB")
            result = query_api.query(self.influx_energy_query(parameter[key]["par"], fil=parameter[key]["filter"] , day=day))
            for table in result:
                for record in table:
                    value = round(record.get_value(), 2)
            start_date, end_date = datevalues.date_values(day)
            logging.info("NEW Calculating {} of day and writing value to daily table".format(key))
            logging.info("{}: {}kWh".format(key, value))

    def update_daily_average_temp(self, parameter, day=None):
        '''
        This function reads the recorded temperature values of a day,
        calculates the mean value and stores it in the daily database.
        '''
        logging.info("Calculation mean temperature and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        try:
            mean_temp = self.get_mean(parameter, start_date)
            logging.info("{}: {}°C".format(parameter, mean_temp))
            if(writeDB):
                self.maria.write_day(start_date, parameter, mean_temp)
            else:
                logging.warning("Not writing to DB")
        except Exception as e:
            logging.error("Something went wrong: " + str(e))

    def get_mean(self, parameter, day=None):
        '''
        Returns a day's mean value of a parameter 
        '''
        logging.debug("Calculation average for {}".format(parameter))
        start_date, end_date = datevalues.date_values(day)
        res = self.maria.read_day(start_date, parameter)
        res = np.array(res)
        return(round(np.mean(res[:,2]),2))

    def daily_updates(self, day):
        '''
        This function performs the daily updates. Day must be given in format
        "2020-10-10",
        '''
        logging.info("Performing daily database updates")
        logging.info(" ")
        self.update_solar_gain(day=day)
        logging.info(" ")
        self.update_pellet_consumption(day=day)
        logging.info(" ")
        self.update_daily_average_temp("OekoAussenTemp", day=day)
        logging.info(" ")
        self.update_electrical(day=day)
        logging.info(" ")

        energies = ["VerbrauchHeizungEG", "VerbrauchHeizungDG", "VerbrauchWW" "VerbrauchStromEg", "VerbrauchStromOg", "VerbrauchStromAllg"]
        for parameter in energies:
            self.update_energy_consumption(parameter, day=day)
            logging.info(" ")

        water = ["VerbrauchGartenwasser"]
        for parameter in water:
            self.update_water_consumption(parameter, day=day)
            logging.info(" ")

        redundancy_deletion = ["OekoStorageFill", "OekoStoragePopper", "OekoCiStatus", "OekoPeStatus"]
        for parameter in redundancy_deletion:
            self.maria.delete_redundancy(parameter, day=day)
            logging.info(" ")


if __name__ == "__main__":
    '''
    When this file is called as a main program, it is a helper for daily jobs.
    To use this file for testing, just call the file withour arguments.
    For daily jobs, ther are arguments:
        mysqldose.py -d day -u
        -d day: Specifiy day in format "2020-10-10", could also be "yesterday"
        -u: perform daily updates
        e.g.; mysqldose.py -d yesterday -u
    '''
    # Creating object
    dr = DailyReport()
    # Varible initialization
    day = None
    update = False
    #Check if arguments are valid
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'd:u')
    except getopt.GetoptError as err:
        logging.error("Arguments error!")
        exit()
    #Parsing arguments
    for o,a in opts:
        if(o == "-d"):
            if(a == "yesterday"):
                day = datetime.date.today() - datetime.timedelta(1)
                day = day.strftime("%Y-%m-%d")
            else:
                try:
                    day = datetime.datetime.strptime(a, "%Y-%m-%d")
                except:
                    logging.error("Invalid date format. Use something like 2020-10-10. Bye")
                    exit()
        elif(o == "-u"):
            update = True


    if(update):
        dr.daily_updates(day)


    #dr.influx_calc_energy("2022-02-19")


    #start_date = datetime.date(2022,11,10)
    #day_count = 16
    #for single_date in (start_date + datetime.timedelta(n) for n in range(day_count)):
    #    print(single_date)
    #    dr.daily_updates(single_date)
    #    dr.update_daily_average_temp("OekoAussenTemp", day=single_date)


    #dr.write('2017-11-12 1:2:3', 'Test', 44.0)
    #result = dr.read_one("OekoKollLeistung", "2018-09-12")
    #print(dr.read_many("OekoAussenTemp", "2020-10-18 18:01%"))
    #print(dr.read_one("OekoAussenTemp"))
