cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/voltrad1/volquotes/historical_data_loader.py >> /var/log/voltrad1/ib_historical_${DATE}.log 2>&1 &
