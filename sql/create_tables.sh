#!/usr/bin/env bash

export USERNAME1=voltrad1
export PGPASSWORD=trustCFA1610

# createuser <username>
# createdb <dbname>
# psql
# psql=# alter user <username> with encrypted password '<password>';

psql -h localhost -d $USERNAME1 -U $USERNAME1 -p 5432 -a -w -f ~/voltrad1/sql/opt_chain_ib.sql
psql -h localhost -d $USERNAME1 -U $USERNAME1 -p 5432 -a -w -f ~/voltrad1/sql/opt_chain_yhoo.sql


#  USAR esta tool para encontrar errores en los data types antes de migrar con python
#
#
#
#       pgloader optchain_ib_expiry_2017-04.db postgresql:///voltrad1