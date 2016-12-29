#!/usr/bin/env bash
cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/voltrad1/volaccounting/vol_grab_accounting.py >> /var/log/voltrad1/ib_accounting_${DATE}.log 2>&1 &
