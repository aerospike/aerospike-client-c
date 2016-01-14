Aerospike C Client Benchmarks
=============================

This project contains the files necessary to build C client benchmarks. 
This program is used to insert data and generate load. 

Build instructions:

    make clean
    make [EVENT_LIB=libev|libuv]

The EVENT_LIB setting must also match the same setting when building the client itself.
If an event library is defined, it must be installed separately.  libev and libuv usually
install into /usr/local/lib.  Most operating systems do not search /usr/local/lib by 
default.  Therefore, the following LD_LIBRARY_PATH setting may be necessary.

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

Build examples:

    make                 # synchronous functionality only
    make EVENT_LIB=libev  # synchronous and asynchronous functionality with libev   
    make EVENT_LIB=libuv  # synchronous and asynchronous functionality with libuv   

The command line usage can be obtained by:

    target/benchmarks -u

Some sample arguments are:

```
# Connect to localhost:3000 using test namespace.
# Read 10% and write 90% of the time using 20 concurrent threads.
# Use 10000000 keys and 50 character string values.
target/benchmarks -h 127.0.0.1 -p 3000 -n test -k 10000000 -o S:50 -w RU,10 -z 20
```

```
# Connect to localhost:3000 using test namespace.
# Read 80% and write 20% of the time using 8 concurrent threads.
# Use 1000000 keys and 1400 length byte array values using a single bin.
# Timeout after 50ms for reads and writes.
# Restrict transactions/second to 2500.
target/benchmarks -h 127.0.0.1 -p 3000 -n test -k 1000000 -o B:1400 -w RU,80 -g 2500 -T 50 -z 8
```

```
# Benchmark asynchronous methods using a single producer thread and 4 selector threads.
# Limit the maximum number of concurrent commands to 200.
# Use and 50% read 50% write pattern.
target/benchmarks -h 127.0.0.1 -p 3000 -n test -k 1000000 -S 1 -o S:50 -w RU,50 -z 1 -async -asyncMaxCommands 200 -asyncSelectorThreads 4
```
