#!/bin/bash

export UNS="test"
export USET="set"

export UNS="test"
export USET="set"
#
export SETBIN="SetBin"
export UFILE="AsLSetStrawman"
export FUN_CR8="asLSetCreate"
export FUN_INS="asLSetInsert"
export FUN_SRCH="asLSetSearch"

VAL=1
USERID=300

SIZES="10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000 10000"
#SIZES="10 20"

for NUM_PUSHES in $SIZES; do
  RAN=$(lua -e "math.randomseed(os.time());print(math.random(77))");
  echo "Pushing: NUM_PUSHES: $NUM_PUSHES RAN: $RAN"
  I=1; while [ $I -le $NUM_PUSHES ]; do
    ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_INS $SETBIN $VAL > /dev/null
    I=$[${I}+1];
    VAL=$[${VAL}+${RAN}];
  done
  RVAL=$[${VAL}-${RAN}]

  I=1; while [ $I -le $NUM_PUSHES ]; do
    RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCH $SETBIN $RVAL)
    if [ $RES -ne $RVAL ]; then
      echo "ERROR: Search: ($RVAL) Returned: ($RES)"
    fi

    I=$[${I}+1];
    RVAL=$[${RVAL}-${RAN}];
  done

  USERID=$[${USERID}+1];
done
