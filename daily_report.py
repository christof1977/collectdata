#!/usr/bin/env python3

import os
import sys
import getopt
import syslog
import configparser
import time
import datetime
import json
import numpy as np
from libby import mysqldose
from libby import datevalues
from libby.remote import udpRemote
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

import logging
logging.basicConfig(level=logging.INFO)
#logging.basicConfig(level=logging.DEBUG)

configfile = "collectdata.ini"
realpath = os.path.realpath(__file__)
basepath = os.path.split(realpath)[0]
configfile = os.path.join(basepath, configfile)

class DailyReport(object):
    def __init__(self):
        self.read_config()
        self.maria = mysqldose.Mysqldose(self.mysqluser, self.mysqlpass, self.mysqlserv, self.mysqldb)
        self.write_maria = False

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
            self.controller = self.config['BASE']['Controller'].split(',')
        except Exception as e:
            logging.error("Configuration error")
            logging.error(e)

    def influx_raw_query(self, parameter, fil=None, day=None):
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "'+ self.influxbucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["topic"] == "'+parameter+'" and r["_field"] == "value")'
        if(fil in ["pos", "neg"]):
            if(fil=="pos"):
                query = query + '|> filter(fn: (r) => r["_value"] > 0.0)'
            else:
                query = query + '|> filter(fn: (r) => r["_value"] <= 0.0)'
        query = query + ' |> keep(columns: ["_time", "_value"])'
        return query

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

    def get_counter_values(self, day=None):
        ''' Holt die Werte von Wassser und Wärmemengenzähler und speichert sie in die Daily-Datenbank
        Die Namen der Zähler werden von den Controllern geliefert und müssen mit den Spaltennamen (Mit Prefix "Zaehler" ) übereinstimmen.
        '''
        start_date, end_date = datevalues.date_values(day)
        try:
            for controller in self.controller:
                logging.info("Getting counter values from " + controller)
                counters = udpRemote(json.dumps({"command":"getCounter"}), addr=controller, port=5005)
                for counter in counters["Counter"]:
                    now = time.strftime('%Y-%m-%d %H:%M:%S')
                    ret = udpRemote(json.dumps({"command":"getCounterValues","Counter":counter}), addr=controller, port=5005)
                    if ret["Data"]["Type"] == "Energy":
                        value = ret["Data"]["Energy"]["Value"]
                        unit = ret["Data"]["Energy"]["Unit"]
                        key = ret["Counter"]
                    elif ret["Data"]["Type"] == "Volume":
                        value = ret["Data"]["Volume"]["Value"]
                        unit = ret["Data"]["Volume"]["Unit"]
                        key = ret["Counter"]
                    logging.info("{} {} {} {}".format(now, key, value, unit))
                    if(self.write_maria):
                        self.maria.write_day(start_date, "Zaehler"+key, value)
                    else:
                        logging.warning("Not writing to DB")
        except Exception as e:
            logging.error("No answer from " + controller)
            logging.error(e)

    def get_electrical_power(self, day=None):
        ''' Reads the current import power from the electrial power meters and stores them to daily value database.
        '''
        start_date, end_date = datevalues.date_values(day)
        try:
            json_string = json.dumps({"command":"getFloors"})
            ret = udpRemote(json_string, addr="piesler", port=5009)
            for floor in ret["Floors"]:
                json_string = json.dumps({"command":"getImportPower","Device":"SDM72","Floor":floor})
                ret = udpRemote(json_string, addr="piesler", port=5009)
                value = ret["Data"]["Energy"]["Value"]
                unit = ret["Data"]["Energy"]["Unit"]
                key = "Strom"+ret["Data"]["Floor"]
                now = time.strftime('%Y-%m-%d %H:%M:%S')
                logging.info("{} {} k{}".format(key, value/1000, unit))
                #self.write_value(now, key, value, unit)
                #publish.single("Power/"+floor+"/"+key, value, hostname="dose")
                if(self.write_maria):
                    self.maria.write_day(start_date, "Zaehler"+key, value)
                else:
                    logging.warning("Not writing to DB")
        except Exception as e:
            logging.error("Error during reading import power")
            #logging.error(e)

    def update_solar_gain(self, day=None):
        '''
        This function reads the solar gain of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        logging.info("Calculating solar gain and writing value to daily table")
        start_date, end_date = datevalues.date_values(day)
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        result = query_api.query(self.influx_energy_query("oekofen/se2/L_pwr", day=day))
        for table in result:
            for record in table:
                pwr = "{:.2f}".format(record["_value"]*1000)
        logging.info("Solarertrag: {}kWh".format(pwr))
        if(self.write_maria):
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
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        query = self.influx_raw_query("oekofen/pe1/L_storage_popper", day=day)
        result = query_api.query(query)
        val = [0]
        for record in result:
            for line in record:
                try:
                    # Take only values with differ from the previous one
                    if(line["_value"] != val[-1]):
                        val.append(line["_value"])
                except:
                    pass
        val.pop(0)
        val = np.array(val)
        diffval = np.diff(val)
        noval = len(diffval)
        diffval = diffval[diffval != -1]
        verbrauch = noval - len(diffval)
        logging.info("Pelletverbrauch: {}kg".format(verbrauch))
        start_date, end_date = datevalues.date_values(day)
        if(self.write_maria):
            self.maria.write_day(start_date, "VerbrauchPellets", verbrauch)
        else:
            logging.warning("Not writing to DB")

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
            if(self.write_maria):
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
        #start_date, end_date = datevalues.date_values(day)
        start_date, end_date = datevalues.date_values_influx(day)
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        query = 'from(bucket: "oekofen") \
                |> range(start: ' + start_date + ', stop: ' + end_date + ') \
                |> filter(fn: (r) => r["tag"] == "' + parameter + '") \
                |> aggregateWindow(every: 24h, fn: mean) \
                |> yield(name: "mean")'
        result = query_api.query(query)
        try:
            for table in result:
                for record in table:
                    mean_temp = round(record["_value"],2)
            logging.info("{}: {}°C".format(parameter, mean_temp))
            if(self.write_maria):
                self.maria.write_day(start_date, parameter, mean_temp)
            else:
                logging.warning("Not writing to DB")
        except Exception as e:
            logging.error("Something went wrong: " + str(e))

    def calculate_daily_values(self, day):
        start_date, end_date = datevalues.date_values(day)
        day_before = start_date -  datetime.timedelta(days=1)
        res_day = self.maria.read_daily_row(start_date)
        res_day_before = self.maria.read_daily_row(day_before)
        parameter = ["HeizungEG", "HeizungDG", "Warmwasser", "Gartenwasser",  "StromEg", "StromOg", "StromAllg"]
        for par in parameter:
            try:
                para = "Zaehler" + par
                if(par == "Gartenwasser"):
                    con = round(res_day[para] - res_day_before[para],3)
                else:
                    con = round(res_day[para] - res_day_before[para],3)/1000
                logging.info("{} Alt: {} Neu: {} Verbrauch: {}".format(par, res_day_before[para], res_day[para], con))
                if(self.write_maria):
                    self.maria.write_day(start_date, "Verbrauch"+par, con)
                else:
                    logging.warning("Not writing to DB")
            except:
                logging.warning("Something went wrong. Maybe no values for given day?")

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
        self.update_daily_average_temp("L_ambient", day=day)
        logging.info(" ")
        self.update_electrical(day=day)
        logging.info(" ")
        self.get_counter_values(day=day)
        logging.info(" ")
        self.get_electrical_power(day=day)
        logging.info(" ")
        self.calculate_daily_values(day=day)

if __name__ == "__main__":
    '''
    If this file is called as a main program, it is a helper for daily jobs.
    To use this file for testing, just call the file without arguments.
    For daily jobs, there are arguments:
        mysqldose.py -d day -u
        -d day: Specifiy day in format "2020-10-10", could also be "yesterday"
        -u: perform daily updates
        e.g.; mysqldose.py -d yesterday -u
        -c: Get counter values (Heizung, Warmwasser, Gartenwasser)
    '''
    # Creating object
    dr = DailyReport()
    # Varible initialization
    day = None
    update = False
    counter_values = False
    electrical_power = False
    solar_gain = False
    pellet_consumption = False
    daily_values = False
    average_temp = False
    #Check if arguments are valid
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'd:uwcespvt')
    except getopt.GetoptError as err:
        logging.error("Arguments error!")
        exit()
    #Parsing arguments
    for o,a in opts:
        logging.info(o)
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
        if(o == "-u"):
            update = True
        if(o == "-w"):
            logging.info("Writing to MariaDB")
            dr.write_maria = True
        if(o == "-c"):
            counter_values = True
        if(o == "-e"):
            electrical_power = True
        if(o == "-s"):
            solar_gain = True
        if(o == "-p"):
            pellet_consumption = True
        if(o == "-v"):
            daily_values = True
        if(o == "-t"):
            average_temp = True

    if(solar_gain):
        dr.update_solar_gain(day=day)

    if(counter_values):
        dr.get_counter_values(day=day)

    if(electrical_power):
        dr.get_electrical_power(day=day)

    if(update):
        dr.daily_updates(day)

    if(pellet_consumption):
        dr.update_pellet_consumption(day=day)

    if(daily_values):
        dr.calculate_daily_values(day=day)

    if(average_temp):
        dr.update_daily_average_temp("L_ambient", day=day)

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
