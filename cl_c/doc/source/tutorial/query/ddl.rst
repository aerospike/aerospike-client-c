********************************
Using the ASQL Utility
********************************

Aerospike provides a Standard Query Language (SQL) interface to perform DDL operations on secondary indexes.

Installation of the client should install the binary 'asql' to your linux path.


ASQL
----
The 'asql' utility should be used as you are used to using the 'mysql' client utlity. If the 'asql' utility is invoked without arguments, it will enter into a prompt mode, where supported SQL commands can be entered in. Typing 'asql --help' will show the command line options 'asql' currently supports.


Creating a secondary index
--------------------------
To create a secondary index, type the following at 'asql' command line:

    Asql > CREATE INDEX iname ON nsname.sname (bname) NUMERIC|STRING

This will create an index named 'iname' on the namespace 'nsname' and setname 'sname' and on the specific bin 'bname'. An index can be either NUMERIC or STRING.

NOTE: The secondary index will be built in the background, and will not be query-able until it is completely built


Dropping a secondary index
--------------------------
To drop a secondary index, type the following at 'asql' command line:

    Asql > DROP INDEX nsname iname

This will drop the index named 'iname' in the namespace 'nsname'.


Listing all secondary indexes in a namespace
--------------------------------------------
To list secondary indexes, type the following at 'asql' command line:

    Asql > SHOW INDEXES nsname

This will list the indexes in the namespace 'nsname'.


Information on a single secondary index
---------------------------------------
To display information on a single secondary index, type the following at 'asql' command line:

    Asql > DESC INDEX nsname iname

This will display information on the index named 'iname' in the namespace 'nsname'.

Statistics on a single secondary index
---------------------------------------
To display statistics on a single secondary index, type the following at 'asql' command line:

    Asql > STAT INDEX nsname iname

This will display statistics on the index named 'iname' in the namespace 'nsname'.


