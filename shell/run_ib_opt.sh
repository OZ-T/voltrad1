cd /tmp
export PYTHONPATH=/home/david/voltrad1
DATE=`date +%d-%m-%y` 
nohup /home/david/anaconda2/bin/python /home/david/voltrad1/volquotes/ib_option_chains_reader.py >> /var/log/voltrad1/ib_chain_opt_${DATE}.log 2>&1 &
