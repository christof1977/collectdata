#!/usr/bin/env python3

import socket
import sys
import json
import select
import logging

from libby.remote import udpRemote

#logging.basicConfig(level=logging.DEBUG)

udpTimeout = 4
addr = 'piesler'
port = 5009

def getcmds():
    valid_cmds = ['getAlive',
                  'getStatus',
                  'getRooms',
                  'getRoomStatus',
                  'setRoomStatus',
                  'getTimer',
                  'setTimer' ]
    return valid_cmds


def getch():
    import sys, tty, termios
    fd = sys.stdin.fileno( )
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setraw(fd)
        ch = sys.stdin.read(1)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return ch


def main():
    valid_cmds = getcmds()
    json_string = json.dumps({"command":"getFloors"})
    ret = udpRemote(json_string, addr=addr, port=port)
    if(ret!=-1):
        try:
            #for floor in ret["Floors"]:
            for floor in ["Allg"]:
                #json_string = json.dumps({"command":"getPhaseValue","Floor":floor,"Phase":1,"Meas":"power_active"})
                json_string = json.dumps({"command":"getImportPower","Device":"SDM72","Floor":floor})
                #json_string = json.dumps({"command":"setSendperm","Value":"On"})
                ret = udpRemote(json_string, addr=addr, port=port)
                if(ret!=-1):
                    try:
                        print(json.dumps(ret,indent=4))
                        if(ret is None):
                            sys.exit(1)
                    except Exception as e:
                        print("ups")
                        print(e)
        except Exception as e:
            print("ups")
            print(e)


if __name__ == "__main__":
   main()

