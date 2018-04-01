#!/usr/bin/env bash
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
/home/david/anaconda2/bin/python /home/david/voltrad1/quotes/yahoo_biz_economic_calendar_us.py >> /var/log/voltrad1/yahoo_ecalendar_${DATE}.log 2>&1 &
