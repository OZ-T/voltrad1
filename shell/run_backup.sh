#!/usr/bin/env bash
cd /tmp
DATE=`date +%y%m%d` 
nohup tar cf /home/david/Dropbox/proyectos/backup_voltrad1/${DATE}.tar.gz --use-compress-prog=pbzip2 /data/voltrad1/ >> /var/log/voltrad1/backup_${DATE}.log 2>&1 &






