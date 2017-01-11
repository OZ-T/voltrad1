#!/usr/bin/env bash
cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/voltrad1/volutils/run.py batch_tic_rpt  >> /var/log/voltrad1/batch_tic_rpt_${DATE}.log 2>&1 &
