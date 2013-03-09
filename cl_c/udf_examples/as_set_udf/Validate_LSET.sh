#!/bin/bash

export UNS="test"
export USET="set"

export SETBIN="SetBin"
export UFILE="AsLSetStickman"
export FUN_CR8="asLSetCreate"
export FUN_INS="asLSetInsert"
export FUN_SRCH="asLSetSearch"
export FUN_DEL="asLSetDelete"

VAL=1
USERID=300

SIZES="10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000 10000"
#SIZES="10 20"

for NUM_PUSHES in $SIZES; do
  ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_CR8 $UNS $USET $SETBIN 0
  RAN=$(lua -e "math.randomseed(os.time());print(math.random(77))");
  echo "Pushing: NUM_PUSHES: $NUM_PUSHES RAN: $RAN"
  I=1; while [ $I -le $NUM_PUSHES ]; do
    ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_INS $UNS $USET $SETBIN 0 $VAL > /dev/null
    I=$[${I}+1];
    VAL=$[${VAL}+${RAN}];
  done
  ORVAL=$[${VAL}-${RAN}];

  NUM_SEARCHES=$NUM_PUSHES
  echo "Pushing: NUM_SEARCHES: $NUM_SEARCHES"
  RVAL=$ORVAL
  I=1; while [ $I -le $NUM_SEARCHES ]; do
    RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCH $SETBIN $RVAL)
    if [ $RES -ne $RVAL ]; then
      echo "ERROR: Search: ($RVAL) Returned: ($RES)"
    fi
    I=$[${I}+1];
    RVAL=$[${RVAL}-${RAN}];
  done

  # Delete one third of what was just added
  NUM_DELS=$[${NUM_PUSHES}/3]
  echo "Pushing: NUM_DELS: $NUM_DELS"
  RVAL=$ORVAL
  I=1; while [ $I -le $NUM_DELS ]; do
    ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_DEL $SETBIN $RVAL >/dev/null
    RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCH $SETBIN $RVAL)
    if [ $RES -eq $RVAL ]; then
      echo "ERROR: Delete: ($RVAL) Returned: ($RES)"
    fi
    I=$[${I}+1];
    RVAL=$[${RVAL}-${RAN}];
  done

  USERID=$[${USERID}+1];
done
