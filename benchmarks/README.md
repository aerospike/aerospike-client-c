Aerospike C Client Benchmarks
=============================

This project contains the files necessary to build C client benchmarks. 
This program is used to insert data and generate load. 

Build instructions:

    make clean
    make

The command line usage can be obtained by:

    target/benchmarks -u

Some sample arguments are:

    # Connect to localhost:3000 using test namespace.
    # Read 10% and write 90% of the time using 20 concurrent threads.
    # Use 10000000 keys and 50 character string values.
    target/benchmarks -h 127.0.0.1 -p 3000 -n test -k 10000000 -o S:50 -w RU,10 -z 20

    # Connect to localhost:3000 using test namespace.
    # Read 80% and write 20% of the time using 8 concurrent threads.
    # Use 1000000 keys and 1400 length byte array values using a single bin.
    # Timeout after 50ms for reads and writes.
    # Restrict transactions/second to 2500.
    target/benchmarks -h 127.0.0.1 -p 3000 -n test -k 1000000 -o B:1400 -w RU,80 -g 2500 -T 50 -z 8
