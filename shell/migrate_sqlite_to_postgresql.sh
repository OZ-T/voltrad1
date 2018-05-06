#!/usr/bin/env bash

export IN_PATH=/data/voltrad1

if [ "$1" == "IB" ]; then
    export SYMBOL_LIST="ES SPY"
    export EXPIRY_LIST="2016-08 2016-09 2016-10 2016-11 2016-12 2017-01 2017-02 2017-03 2017-04 2017-05 2017-06 2017-07 2017-08 2017-09 2017-10 2017-11 2017-12 2018-01 2018-02 2018-03"
    for VARIABLE1 in $SYMBOL_LIST
    do
        for VARIABLE2 in EXPIRY_LIST
        do
            echo "/home/david/shell/p mv_ib_opt $VARIABLE1 $VARIABLE2 "
        done
    done
fi
if [ "$1" == "YHOO" ]; then
    export SYMBOL_LIST="BND EMB FXA FXE "
    export EXPIRY_LIST="2016-08 2016-09 2016-10 2016-11 2016-12 2017-01 2017-02 2017-03 2017-04 2017-05 2017-06 2017-07 2017-08 2017-09 2017-10 2017-11 2017-12 2018-01 2018-02 2018-03"
    for VARIABLE1 in $SYMBOL_LIST
    do
        for VARIABLE2 in EXPIRY_LIST
        do
            echo "/home/david/shell/p mv_yhoo_opt $VARIABLE1 $VARIABLE2 "
        done
    done
fi
