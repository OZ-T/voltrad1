export PYTHONPATH=/home/david/python/voltrad1
DATE=`date +%d-%m-%y` 
#/home/david/anaconda2/bin/python /home/david/python/voltrad1/volquotes/yahoo_option_chains_reader.py
/home/david/anaconda2/bin/python /home/david/python/voltrad1/volquotes/yahoo_option_chains_reader_fix_beautifulsoup.py >> /var/log/voltrad1/yahoo_opt_${DATE}.log 2>&1 &
