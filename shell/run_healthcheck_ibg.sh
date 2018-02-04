#!/usr/bin/env bash
cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
#/home/david/anaconda3/bin/python /home/david/voltrad1/ibutils/run_ib_healthcheck.py
NOW1=`date +%d-%m-%y\ %H:%M:%S`
if [[ $(( /home/david/anaconda3/bin/python /home/david/voltrad1/ibutils/run_ib_healthcheck.py ) 2>&1 | grep "Error" ) ]]; then
    echo $NOW1 " Something wrong with IBG. Restarting the service ..."
    /home/david/shell/gw.sh stop sim 
    /home/david/shell/gw.sh stop sim 
    /home/david/shell/gw.sh start sim 
else
    echo $NOW1 "IBG is answering request. Nothing to do for now."
fi

# TODO: check this also
# Connectivity between Trader Workstation and server is broken
