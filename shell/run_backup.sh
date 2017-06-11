#!/usr/bin/env bash
BK_DATE=`date +%Y_%m_%d__%H_%M_%S`
cd /mnt/volume-fra1-01/voltrad1
nohup tar cf /mnt/volume-fra1-01/backup/${BK_DATE}.tar.gz --use-compress-prog=pbzip2 *.db >> /var/log/voltrad1/backup_${BK_DATE}.log 2>&1 &







