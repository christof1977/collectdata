#!/usr/bin/env python3

import socket
import sys
import json
import select
import logging
from urllib.request import urlopen


logging.basicConfig(level=logging.ERROR)

udpTimeout = 2

def udpRemote(msg, **kwargs):
    # Setzt einen JSON-String in Richtung Ampi per UDP ab
    # msg: JSON-String nach Ampi-Spec
    # udpSocket: wenn da ein Socket übergeben wird, wird halt der hergenommen
    # addr, port: Gibt's keinen Socket, wird einer mit addr, port aufgemacht
    # Wenn nix übergeben wird, gibt's halt einen Standard-Socket

    addr = kwargs.get('addr')
    port = kwargs.get('port')
    logging.info("Öffne Socket")
    try:
        udpSocket = socket.socket( socket.AF_INET,  socket.SOCK_DGRAM )
        udpSocket.setblocking(0)
    except Exception as e:
        logging.info(str(e))
    try:
        ret = -1
        logging.info("So laut des heit: %s", msg)
        ready = select.select([], [udpSocket], [], udpTimeout)
        if(ready[1]):
            udpSocket.sendto( msg.encode(), (addr,port) )
            logging.info("Gesendet")
        ready = select.select([udpSocket], [], [], udpTimeout)
        if(ready[0]):
            data, addr = udpSocket.recvfrom(1024)
            logging.info(data.decode())
            ret = data.decode()
            return(ret)
    except Exception as e:
        logging.info(str(e))
        return -1

def main():
    if len(sys.argv) == 1:
        addr = 'heizungeg'
        port = 5005
    elif len(sys.argv) == 2:
        addr = sys.argv[1]
        port = 5005
    elif len(sys.argv) == 3:
        addr = sys.argv[1]
        port = int(sys.argv[2])
    else:
        logging.error("Wrong number of arguments, exit.")
        exit()

    try:
        # Try, if the controller answers via the really cool and modern REST interface
        with urlopen("http://" + addr + ":" + str(port)) as url:
            ret = json.load(url)
    except:
        try:
            # Try, if the controller is old-fashined reachable via udp-server
            json_string = '{"command" : "getAlive"}\n'
            ret = json.loads(udpRemote(json_string, addr=addr, port=port))
        except:
            ret = {}
            ret["answer"] = "Nix"
    if(ret["answer"] == "Freilich"):
        print(json.dumps(ret))
        sys.exit(0)
    else:
        print(json.dumps(ret))
        sys.exit(100)




if __name__ == "__main__":
   main()



