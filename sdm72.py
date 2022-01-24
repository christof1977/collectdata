#!/usr/bin/env python3
import datetime
ts_null = datetime.datetime.now()
import sys
sys.path.insert(0, './sdm_modbus/src')
sys.path.insert(0, './sdm_modbus')
import argparse
import json
import sdm_modbus
import time
import logging
import threading
from threading import Thread
from libby import udp_broadcast
import math


print(datetime.datetime.now()-ts_null)
ts_null = datetime.datetime.now()
server = "dose"
udpBcPort =  6664
logger = logging.getLogger('Stromzaehler')
logger.setLevel(logging.DEBUG)
#logger.setLevel(logging.INFO)


device = "/dev/ttyAMA0"
stopbits = 1
parity = "N"
baud = 1200
timeout = 2
unit_eg = 1
unit_allg = 2
unit_og = 3

meter_eg = sdm_modbus.SDM72(
            device=device,
            stopbits=stopbits,
            parity=parity,
            baud=baud,
            timeout=timeout,
            unit=unit_eg
        )
        
print(datetime.datetime.now()-ts_null)
ts_null = datetime.datetime.now()
p1_cur = round(meter_eg.read("p1_current"), 3)
p1_vol = round(meter_eg.read("p1_voltage"), 3)
p1_pwr_act = round(meter_eg.read("p1_power_active"), 3)
p1_pwr_app = round(meter_eg.read("p1_power_apparent"), 3)
p1_pwr_rea = round(meter_eg.read("p1_power_reactive"), 3)
p1_pf = round(meter_eg.read("p1_power_factor"), 3)
p1_angle = round(meter_eg.read("p1_phase_angle"), 3)

print("Time L1: ", datetime.datetime.now()-ts_null)
ts_null = datetime.datetime.now()

p2_cur = round(meter_eg.read("p2_current"), 3)
p2_vol = round(meter_eg.read("p2_voltage"), 3)
p2_pwr_act = round(meter_eg.read("p2_power_active"), 3)
p2_pwr_app = round(meter_eg.read("p2_power_apparent"), 3)
p2_pwr_rea = round(meter_eg.read("p2_power_reactive"), 3)
p2_pf = round(meter_eg.read("p2_power_factor"), 3)
p2_angle = round(meter_eg.read("p2_phase_angle"), 3)

print("Time L2: ", datetime.datetime.now()-ts_null)
ts_null = datetime.datetime.now()

p3_cur = round(meter_eg.read("p3_current"), 3)
p3_vol = round(meter_eg.read("p3_voltage"), 3)
p3_pwr_act = round(meter_eg.read("p3_power_active"), 3)
p3_pwr_app = round(meter_eg.read("p3_power_apparent"), 3)
p3_pwr_rea = round(meter_eg.read("p3_power_reactive"), 3)
p3_pf = round(meter_eg.read("p3_power_factor"), 3)
p3_angle = round(meter_eg.read("p3_phase_angle"), 3)

print("Time L3: ",datetime.datetime.now()-ts_null)
ts_null = datetime.datetime.now()
tot_pwr = round(meter_eg.read("total_system_power"), 3)
tot_imp_pwr = round(meter_eg.read("total_import_active_power"), 3)

print("Time Pwr ", datetime.datetime.now()-ts_null)
ts_null = datetime.datetime.now()
print("P1 Cur:      ", p1_cur, "A")
print("P1 Vol:      ", p1_vol, "V")
print("P1 Pwr Act:  ", p1_pwr_act, "W")
print("P1 Pwr App:  ", p1_pwr_app, "VA")
print("S =          ", round(math.sqrt(p1_pwr_act ** 2 + p1_pwr_rea ** 2),3), "VA")
print("P1 Pwr Rea:  ", p1_pwr_rea, "VAr")
print("P1 Pwr Fac:  ", p1_pf)
print("PF =         ", round(abs(p1_pwr_act/p1_pwr_app),3))
print("P1 Pha Ang:  ", p1_angle, "°")
print("Calcuated")
print("P1 Pwr:      ", round(p1_cur * p1_vol, 3), "VA")

print("***************************************")

print("P2 Cur:      ", p2_cur, "A")
print("P2 Vol:      ", p2_vol, "V")
print("P2 Pwr Act:  ", p2_pwr_act, "W")
print("P2 Pwr App:  ", p2_pwr_app, "VA")
print("S =          ", round(math.sqrt(p2_pwr_act ** 2 + p2_pwr_rea ** 2),3), "VA")
print("P2 Pwr Rea:  ", p2_pwr_rea, "VAr")
print("P2 Pwr Fac:  ", p2_pf)
print("PF =         ", round(abs(p2_pwr_act/p2_pwr_app),3))
print("P2 Pha Ang:  ", p2_angle, "°")
print("Calcuated")
print("P2 Pwr:      ", round(p2_cur * p2_vol, 3), "VA")

print("***************************************")

print("P3 Cur:      ", p3_cur, "A")
print("P3 Vol:      ", p3_vol, "V")
print("P3 Pwr Act:  ", p3_pwr_act, "W")
print("P3 Pwr App:  ", p3_pwr_app, "VA")
print("S =          ", round(math.sqrt(p3_pwr_act ** 2 + p3_pwr_rea ** 2),3), "VA")
print("P3 Pwr Rea:  ", p3_pwr_rea, "VAr")
print("P3 Pwr Fac:  ", p3_pf)
print("PF =         ", round(abs(p3_pwr_act/p3_pwr_app),3))
print("P3 Pha Ang:  ", p3_angle, "°")
print("Calcuated")
print("P3 Pwr:      ", round(p3_cur * p3_vol, 3), "VA")

print("***************************************")

print("Total Pwr Meas", tot_pwr)
print("Total Pwr Cal", p1_pwr_act + p2_pwr_act + p3_pwr_act)
print("Total Imp Pwr ", tot_imp_pwr)
