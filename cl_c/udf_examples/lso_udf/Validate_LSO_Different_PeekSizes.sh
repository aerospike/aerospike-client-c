#!/bin/bash

export UNS="test"
export USET="set"
export LSOBIN="LsoBin"

export UFILE="LsoStoneman"
export PUSHUDF="stumbleCompress5"
export PEEKUDF="stumbleUnCompress5"
export UDF1_ARGS=""
export FUN_INSWU="stackPushWithUDF"
export FUN_SRCHWU="stackPeekWithUDF"

URLID=1000
URLID_MOD=9
CREATED=1400000000
METH_A=50000
METH_B=7000000
STATUS=1

USERID=100
for NUM_PUSHES in 10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000 10000; do
  I=1; while [ $I -le $NUM_PUSHES ]; do
    TUPLE="[${URLID},${CREATED},${METH_A},${METH_B},${STATUS}]"
    ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_INSWU $LSOBIN ${TUPLE} $PUSHUDF $UDF1_ARGS >/dev/null
    I=$[${I}+1];
    if [ $[${I}%${URLID_MOD}] -eq 0 ]; then URLID=$[${URLID}+1]; fi
    CREATED=$[${CREATED}+1];
    METH_A=$[${METH_A}+1];
    METH_B=$[${METH_B}+1];
    STATUS=$[${STATUS}+1];
  done
  START_STATUS=$[${STATUS}-1]
  RAN=$(lua -e "math.randomseed(os.time());print(math.random(10))");
  for SIZE in 10 20 50 100 150 200 250 300 400 500 600 700 800 1000 2000 10000; do
    RAN=$(lua -e "math.randomseed(os.time() + ${RAN});print(math.random(10))");
    ASK=$[${SIZE}-${RAN}]
    if [ $ASK -gt $NUM_PUSHES ]; then ASK=0; fi
    RES=$(ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCHWU $LSOBIN $ASK $PEEKUDF $UDF1_ARGS | tr \] \\n | grep \\[ | wc -l)
    EXPECT=$ASK
    if [ $ASK -eq 0 ]; then EXPECT=$NUM_PUSHES; fi
    DIFF=$[${RES}-${EXPECT}];
    echo "SIZE: $NUM_PUSHES USERID: $USERID [PEEK: $EXPECT GOT: $RES DIFF: $DIFF]"
    ascli udf-record-apply $UNS $USET $USERID $UFILE $FUN_SRCHWU $LSOBIN $ASK $PEEKUDF | tr \] \\n | grep \\[ | rev | cut -f 1 -d ,  | rev | while read E_STATUS; do 
      if [ $E_STATUS -ne $START_STATUS ]; then
        echo "ERROR: ESTATUS: $E_STATUS START_STATUS: $START_STATUS"
      fi
      START_STATUS=$[${START_STATUS}-1];
    done
    if [ $SIZE -gt $NUM_PUSHES ]; then break; fi
  done
  USERID=$[${USERID}+1];
done
