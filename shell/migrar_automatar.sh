cd /mnt/volume-fra1-01/voltrad1
p migrate_h5_optchain_yhoo_sqlite "QQQ"
p migrate_h5_optchain_yhoo_sqlite "USO"
p migrate_h5_optchain_yhoo_sqlite "UNG"
p migrate_h5_optchain_yhoo_sqlite "GLD"
p migrate_h5_optchain_yhoo_sqlite "FXE"
p migrate_h5_optchain_yhoo_sqlite "FXA"
p migrate_h5_optchain_yhoo_sqlite "FXY"
BK_DATE=`date +%Y_%m_%d__%H_%M_%S`
tar cf /mnt/volume-fra1-01/backup/${BK_DATE}.tar.bz2 --use-compress-prog=pbzip2 *.db
p migrate_h5_optchain_yhoo_sqlite "FXF"
p migrate_h5_optchain_yhoo_sqlite "VNQ"
p migrate_h5_optchain_yhoo_sqlite "IYR"
p migrate_h5_optchain_yhoo_sqlite "HYG"
p migrate_h5_optchain_yhoo_sqlite "TLT"
p migrate_h5_optchain_yhoo_sqlite "LQD"
p migrate_h5_optchain_yhoo_sqlite "EMB"
p migrate_h5_optchain_yhoo_sqlite "BND"
p migrate_h5_optchain_yhoo_sqlite "UVX"
p migrate_h5_optchain_yhoo_sqlite "^VIX"
p migrate_h5_optchain_yhoo_sqlite "VXX"
BK_DATE=`date +%Y_%m_%d__%H_%M_%S`
tar cf /mnt/volume-fra1-01/backup/${BK_DATE}.tar.bz2 --use-compress-prog=pbzip2 *.db