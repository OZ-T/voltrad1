#!/usr/bin/env bash
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
#/home/david/anaconda2/bin/python /home/david/python/voltrad1/volquotes/yahoo_option_chains_reader.py
/home/david/anaconda2/bin/python /home/david/voltrad1/volquotes/yahoo_option_chains_reader.py >> /var/log/voltrad1/yahoo_opt_${DATE}.log 2>&1 &
