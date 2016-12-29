#!/usr/bin/env bash
cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/voltrad1/volblotter/vol_grab_orders.py >> /var/log/voltrad1/ib_orders_${DATE}.log 2>&1 &
