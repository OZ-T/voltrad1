export PYTHONPATH=/home/david/python/voltrad1
DATE=`date +%d-%m-%y` 
/home/david/anaconda2/bin/python /home/david/python/voltrad1/volquotes/yahoo_biz_economic_calendar_us.py >> /var/log/voltrad1/yahoo_ecalendar_${DATE}.log 2>&1 &
