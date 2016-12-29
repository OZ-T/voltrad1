#!/usr/bin/env bash
cd /tmp
export PYTHONPATH=/home/david/python
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/python/sysIB/swigibpy_example1.py >> /var/log/voltrad1/ib_chain_opt_${DATE}.log 2>&1 &
