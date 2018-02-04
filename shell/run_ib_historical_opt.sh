#!/usr/bin/env bash
cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/voltrad1/quotes/historical_opt_chains_loader.py >> /var/log/voltrad1/ib_historical_opt_${DATE}.log 2>&1 &
