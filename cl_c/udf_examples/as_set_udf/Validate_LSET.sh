#!/bin/bash

export UNS="test"
export USET="set"

export UFILE="AsLSetStoneman"
export FUN_CR8="asLSetCreate"
export FUN_INS="asLSetInsert"
export FUN_SRCH="asLSetSearch"
export FUN_DEL="asLSetDelete"

export SETBIN="SetBin"

# DEFAULTS
VAL=1
USERID=3000
SIZES="10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000 10000"

DEBUG=false
DO_SEARCHES=true
DO_DELETES=true

if [ -n "$1" ]; then # roll your own size
  USERID=$(date +%s) # makes a unique USERID
  SIZES="$1"
fi

for NUM_PUSHES in $SIZES; do
  RAN=$(lua -e "math.randomseed(os.time());print(math.random(77))");
  echo "Pushing: USERID: $USERID NUM_PUSHES: $NUM_PUSHES RAN: $RAN"
  I=1; while [ $I -le $NUM_PUSHES ]; do
    DRES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_INS $SETBIN $VAL)
    if $DEBUG; then echo "INSERT: DRES: ${DRES}"; fi
    I=$[${I}+1];
    VAL=$[${VAL}+${RAN}];
  done
  ORVAL=$[${VAL}-${RAN}];

  if $DO_SEARCHES; then
    NUM_SEARCHES=$NUM_PUSHES
    echo "Searching: NUM_SEARCHES: $NUM_SEARCHES"
    RVAL=$ORVAL
    I=1; while [ $I -le $NUM_SEARCHES ]; do
      RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCH $SETBIN $RVAL)
      TYPE=$(lua -e "print(type($RES))" 2>&1)
      if [ $TYPE == "number" ]; then
        if [ $RES -ne $RVAL ]; then
          echo "ERROR: USERID: $USERID Search: ($RVAL) Returned: ($RES)"
        fi
      else
        echo "SEARCH: GENERAL ERROR: ${RES}"
      fi
      I=$[${I}+1];
      RVAL=$[${RVAL}-${RAN}];
    done
  fi

  if $DO_DELETES; then
    # Delete one third of what was just added
    NUM_DELS=$[${NUM_PUSHES}/3]
    echo "Deleting: NUM_DELS: $NUM_DELS"
    RVAL=$ORVAL
    I=1; while [ $I -le $NUM_DELS ]; do
      DRES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_DEL $SETBIN $RVAL)
      if $DEBUG; then echo "DELETE: DRES: ${DRES}"; fi
      RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCH $SETBIN $RVAL)
      TYPE=$(lua -e "print(type($RES))" 2>&1)
      if [ $TYPE == "number" ]; then
        if [ $RES -eq $RVAL ]; then
          echo "ERROR: USERID: $USERID Delete: ($RVAL) Returned: ($RES)"
        fi
      fi
      I=$[${I}+1];
      RVAL=$[${RVAL}-${RAN}];
    done
  fi

  USERID=$[${USERID}+1];
done
