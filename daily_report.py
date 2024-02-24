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
import requests
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

    def influx_query(self, parameter, number=None, fil=None, day=None, bucket=None):
        if bucket is None:
            bucket = self.influxbucket
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "'+ bucket +'") \
                |> range(start:'+start_date+', stop: '+ end_date+') \
                      |> filter(fn: (r) => r["tag"] == "'+parameter+'" and r["_field"] == "value")'
        if number is not None:
            query = query + '|> filter(fn: (r) => r["id"] == "' + str(number) + '")'
        if(fil in ["pos", "neg"]):
            if(fil=="pos"):
                query = query + '|> filter(fn: (r) => r["_value"] >= 0.0)'
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

    def influx_energy_query(self, parameter, number=None, fil=None, day=None, bucket=None):
        if bucket is None:
            bucket = self.influxbucket
        start_date, end_date = datevalues.date_values_influx(day)
        query = 'from(bucket: "{}") \
                 |> range(start:{}, stop: {}) \
                 |> filter(fn: (r) => r["tag"] == "{}" and r["_field"] == "value")'.format(bucket, start_date, end_date, parameter)
        if number is not None:
            query = '{} |> filter(fn: (r) => r["id"] == "{}")'.format(query, number)
        if(fil in ["pos", "neg"]):
            if(fil=="pos"):
                fil = '|> filter(fn: (r) => r["_value"] >= 0.0)'
            else:
                fil = '|> filter(fn: (r) => r["_value"] <= 0.0)'
        else:
            fil = ""
        query = '{} \
                 {} \
                 |> window(every: 1h) \
                 |> integral(unit: 1h) \
                 |> group() \
                 |> sum() \
                 |> map(fn: (r) => ({{ r with _value: r._value / 1000.0}}))'.format(query, fil)
        return query

    def get_counter_values(self, day=None):
        ''' Holt die Werte von Wassser und Wärmemengenzähler und speichert sie in die Daily-Datenbank
        Die Namen der Zähler werden von den Controllern geliefert und müssen mit den Spaltennamen (Mit Prefix "Zaehler" ) übereinstimmen.
        '''

        def _get_values(ret):
            now = time.strftime('%Y-%m-%d %H:%M:%S')
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

        start_date, end_date = datevalues.date_values(day)
        for controller in self.controller:
            try:
                # Probieren, ob es noch den UDP-Server im Controller gibt
                counters = udpRemote(json.dumps({"command":"getCounter"}), addr=controller, port=5005)
                for counter in counters["Counter"]:
                    _get_values(udpRemote(json.dumps({"command":"getCounterValues","Counter":counter}), addr=controller, port=5005))
            except TypeError:
                # Wenn es keine UDP-Server mehr gibt, gibt es hoffentlich die neue REST-API
                api_url = "http://" + controller + ":5000/counter"
                counters = requests.get(api_url).json()
                for counter in counters["Counter"]:
                    api_url = "http://" + controller + ":5000/counter/" + counter
                    _get_values(requests.get(api_url).json())
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
        except Exception as e:
            logging.error("Error during reading import power")
            #logging.error(e)

    def update_solar_gain(self, day=None):
        '''
        This function reads the solar gain of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
        start_date, end_date = datevalues.date_values(day)
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        result = query_api.query(self.influx_energy_query("L_pwr", bucket="oekofen", day=day))
        for table in result:
            for record in table:
                pwr = "{:.2f}".format(record["_value"]*1000)
        logging.info("Solarertrag: {}kWh".format(pwr))
        if(self.write_maria):
            self.maria.write_day(start_date, "Solarertrag", pwr)
        return result

    def update_pellet_consumption(self, day=None):
        '''
        This function reads the pellet consumption of a given day out of the
        messwert database table and stores the sum in the daily database
        If no day is given, today is chosen.
        '''
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

    def influx_calc_energy(self, day=None):
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        start_date, end_date = datevalues.date_values_influx(day)
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
        parameter = {"VerbrauchStromHaus":{"bucket":"e3dc","par":"EMS_POWER_HOME","filter":None},
                "PVErtragAc":{"bucket":"e3dc","par":"EMS_POWER_PV","filter":None},
                "PVErtragDcOst":{"bucket":"e3dc","par":"PVI_DC_POWER","id":0,"filter":None},
                "PVErtragDcWest":{"bucket":"e3dc","par":"PVI_DC_POWER","id":1,"filter":None},
                "Netzbezug":{"bucket":"e3dc","par":"EMS_POWER_GRID","filter":"pos"},
                "Netzeinspeisung":{"bucket":"e3dc","par":"EMS_POWER_GRID","filter":"neg"},
                "Batterieladung":{"bucket":"e3dc","par":"EMS_POWER_BAT","filter":"pos"},
                "Batterieentladung":{"bucket":"e3dc","par":"EMS_POWER_BAT","filter":"neg"}}
        for key in parameter:
            try:
                bucket = parameter[key]["bucket"]
            except:
                bucket = self.influxbucket
            try:
                number = parameter[key]["id"]
            except:
                number = None
            #result = query_api.query(self.influx_query(parameter[key]["par"], fil=parameter[key]["filter"], number=number, day=day, bucket=bucket))
            #for table in result:
            #    for record in table:
            #        value = round(record.get_value(), 2)
            #start_date, end_date = datevalues.date_values(day)
            #logging.info("Calculating {} of day and writing value to daily table".format(key))
            #logging.info("{}: {}kWh".format(key, value))
            result = query_api.query(self.influx_energy_query(parameter[key]["par"], fil=parameter[key]["filter"], number=number, day=day, bucket=bucket))
            for table in result:
                for record in table:
                    value = round(record.get_value(), 2)
            start_date, end_date = datevalues.date_values(day)
            #logging.info("NEW Calculating {} of day and writing value to daily table".format(key))
            logging.info("{}: {}kWh (neu)".format(key, value))
            if(self.write_maria):
                self.maria.write_day(start_date, key, value)

    def update_daily_average_temp(self, parameter, bucket=None, day=None):
        '''
        This function reads the recorded temperature values of a day,
        calculates the mean value and stores it in the daily database.
        '''
        if bucket is None:
            bucket = self.influxbucket
        start_date_maria, end_date_maria = datevalues.date_values(day)
        start_date, end_date = datevalues.date_values_influx(day)
        client = InfluxDBClient(url=self.influxserv, token=self.influxtoken, org=self.influxorg)
        query_api = client.query_api()
        query = 'from(bucket: "{}") \
                |> range(start: {}, stop: {}) \
                |> filter(fn: (r) => r["tag"] == "{}") \
                |> aggregateWindow(every: 24h, fn: mean) \
                |> yield(name: "mean")'.format(bucket, start_date, end_date, parameter)
        result = query_api.query(query)
        try:
            for table in result:
                for record in table:
                    mean_temp = round(record["_value"],2)
            logging.info("{}: {}°C".format(parameter, mean_temp))
            if(self.write_maria):
                if parameter == "L_ambient":
                    parameter = "OekoAussenTemp"
                self.maria.write_day(start_date_maria, parameter, mean_temp)
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
        self.update_daily_average_temp("L_ambient", bucket="oekofen", day=day)
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
    energy = False
    #Check if arguments are valid
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, 'd:uwcespvtn')
    except getopt.GetoptError as err:
        logging.error("Arguments error!")
        exit()
    #Parsing arguments
    logging.info("Arguments passed:")
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
        if(o == "-n"):
            energy = True

    if not dr.write_maria:
        logging.warning("Not writing values to database")

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
        dr.update_daily_average_temp("L_ambient", bucket="oekofen", day=day)

    if(energy):
        dr.update_electrical(day=day)

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
