#!/usr/bin/env bash
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
/home/david/anaconda2/bin/python /home/david/voltrad1/volquotes/ivolatility_scrapper.py >> /var/log/voltrad1/ivol_${DATE}.log 2>&1 &
