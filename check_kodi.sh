#!/bin/bash


ANS=$(curl -s --data-binary  '{"jsonrpc":"2.0","id":1,"method":"JSONRPC.Version"}'  -H 'content-type: application/json;' http://$1/jsonrpc)
if [[ $ANS == *"version"* ]]; then
	echo "Kodi on $1 is running"
	exit 0
else
	echo "Kodi on $1 is NOT running"
	exit 1
fi
