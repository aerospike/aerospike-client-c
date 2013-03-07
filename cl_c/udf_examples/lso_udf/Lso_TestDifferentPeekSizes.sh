#!/bin/bash

URLID=1000
URLID_MOD=9
CREATED=1400000000
METH_A=50000
METH_B=7000000
STATUS=1

USERID=100
for NUM_PUSHES in 10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000; do
  I=1;
  while [ $I -le $NUM_PUSHES ]; do
    TUPLE="[${URLID},${CREATED},${METH_A},${METH_B},${STATUS}]"
    ascli udf-record-apply $UNS $USET $USERID $UFILE stackPush $LSOBIN $TUPLE > /dev/null
    I=$[${I}+1];
    if [ $[${I}%${URLID_MOD}] -eq 0 ]; then URLID=$[${URLID}+1]; fi
    CREATED=$[${CREATED}+1];
    METH_A=$[${METH_A}+1];
    METH_B=$[${METH_B}+1];
    STATUS=$[${STATUS}+1];
  done
  for SIZE in 10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000; do
    ASK=$[${SIZE}-2]
    if [ $SIZE -gt $NUM_PUSHES ]; then ASK=0; fi
    RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE stackPeek $LSOBIN $ASK | tr \] \\n | wc -l)
    if [ $ASK -eq 0 ]; then
      echo "PEEK: $SIZE GOT: $RES  DIFF: " $[${RES}-${NUM_PUSHES}]
    else
      echo "PEEK: $SIZE GOT: $RES  DIFF: " $[${RES}-${SIZE}]
    fi
    if [ $SIZE -gt $NUM_PUSHES ]; then break; fi
  done
  USERID=$[${USERID}+1];
done
