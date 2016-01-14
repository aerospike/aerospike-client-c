# Aerospike Client Examples

This directory contains a collection of examples of using the Aerospike client.

## Build

To build all examples:

	$ make [EVENT_LIB=libev|libuv]

The EVENT_LIB setting must also match the same setting when building the client itself.
If an event library is defined, it must be installed separately.  libev and libuv usually
install into /usr/local/lib.  Most operating systems do not search /usr/local/lib by 
default.  Therefore, the following LD_LIBRARY_PATH setting may be necessary.

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

To build a specific example:

	$ make [EVENT_LIB=libev|libuv] -C {example}

## Run

To run all examples:

	$ make [EVENT_LIB=libev|libuv] [AS_HOST=<server IP address>] run
	
To run a specific example:

	$ make [EVENT_LIB=libev|libuv] [AS_HOST=<server IP address>] -C {example} run


# Summary of Examples

The examples are intended to demonstrate client API usage. They do not
exhaustively cover all features. Each example focuses on a particular API call
or set of calls, although all use helper functions (in example_utils and
sometimes local functions) that use other API calls. Some API calls are used
only in example_utils:

	aerospike_connect()
	aerospike_destroy()
	aerospike_udf_put()
	aerospike_udf_remove()
... and usage of as_record_iterator.

All examples clean up after themselves, leaving the database as they found it.


## Basic Examples

These examples each use a single record to demonstrate particular API calls.


### append

	aerospike_key_operate()

This example demonstrates aerospike_key_operate() for append and prepend
operations. It demonstrates that such operations create a record or bin that did
not previously exist, with the data to append/prepend as the initial bin value.
It also demonstrates that we can only append/prepend strings to string values,
and "raw bytes" to raw byte values, otherwise the operation will return error
code AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE.


### expire

	aerospike_key_put()
	aerospike_key_exists()

This example demonstrates writing a record to the database with its TTL (time to
live) value set. It uses aerospike_key_put() to write the record and then
aerospike_key_exists() to verify first that the record was written to the
database and then after waiting past the TTL, that the record expired.


### generation

	aerospike_key_put()
	aerospike_key_get()

This example demonstrates writing a record to the database using non-default
generation policies. It writes (creates) a record, reads the record back noting
the generation, then writes the record again using that generation and requiring
generation match. It then repeats the process, but uses an incorrect generation
to show error code AEROSPIKE_ERR_RECORD_GENERATION returned. The record is
written once more to demonstrate policy requiring that the specified generation
is greater than that of the record in the database.


### get

	aerospike_key_get()
	aerospike_key_select()
	aerospike_key_exists()
	aerospike_key_put()

This example demonstrates reading a record from the database using both
aerospike_key_get() to retrieve the whole record, and aerospike_key_select() to
retrieve particular bins. It shows error code AEROSPIKE_ERR_RECORD_NOT_FOUND is
returned if the record does not exist. It also shows a bin with null value is
returned if a non-existent bin is retrieved via aerospike_key_select(). It also
demonstrates that aerospike_key_exists() returns metadata but no bin data if a
record exists.


### incr

	aerospike_key_operate()

This example demonstrates aerospike_key_operate() for arithmetic operations. It
demonstrates that such operations create a record or bin that did not previously
exist, with the integer to add as the initial bin value. It also demonstrates
that we can only add to integer values, otherwise the operation will return
error code AEROSPIKE_ERR_BIN_INCOMPATIBLE_TYPE. Finally it demonstrates the
combination of arithmetic and read operations in the same transaction, in order
to perform an atomic arithmetic operation.


### lset (3.0 feature)

	aerospike_lset_add()
	aerospike_lset_addall()
	aerospike_lset_size()
	aerospike_lset_filter()
	aerospike_lset_exists()
	aerospike_lset_destroy()

This example demonstrates manipulation of a "large set" record bin. It shows how
to add values using aerospike_lset_add() and aerospike_lset_addall(), how to get
the number of elements (size) using aerospike_lset_size(), and how to check the
existence of a particular value using aerospike_lset_exists(). It also shows how
to return all the values using aerospike_lset_filter(), and how to remove an
entire lset using aerospike_lset_destroy().


### lstack (3.0 feature)

	aerospike_lstack_push()
	aerospike_lstack_pushall()
	aerospike_lstack_size()
	aerospike_lstack_peek()
	aerospike_lstack_set_capacity()
	aerospike_lstack_get_capacity()
	aerospike_lstack_destroy()

This example demonstrates manipulation of a "large stack" record bin. It shows
how to push values using aerospike_lstack_push() and aerospike_lstack_pushall(),
how to get the number of elements (size) using aerospike_lstack_size(), and how
to peek to a specified depth using aerospike_lstack_peek(). It also shows how to
set and query capacity using aerospike_lstack_set_capacity() and
aerospike_lstack_get_capacity(), and how to remove an entire lstack using
aerospike_lstack_destroy().


### put

	aerospike_key_put()
	aerospike_key_remove()

This example demonstrates writing a record to the database using
aerospike_key_put(). It shows that bins are written independently, and that bin
value types may be changed when a bin is rewritten. It also shows how to remove
a bin. It demonstrates the non-default AS_POLICY_EXISTS_CREATE write policy,
showing that error code AEROSPIKE_ERR_RECORD_EXISTS is returned if the record
exists. It then uses aerospike_key_remove() to delete the record, then shows
the next create succeed.


### touch

	aerospike_key_operate()
	aerospike_key_put()

This example writes a record to the database with its TTL set, and reads it back
to show the TTL. It then demonstrates aerospike_key_operate() with a touch
operation, to reset the TTL to a different value. Finally it reads the record
again to show that the TTL value was updated.


### udf (3.0 feature)

	aerospike_key_apply()
	aerospike_key_put()

This example demonstrates the application of UDFs (user defined functions) to a
record in the database. It registers a UDF package and writes a record in the
database. Using aerospike_key_apply() it applies a simple UDF with no arguments
or return value, to perform an arithmetic operation on one of the bin values,
then reads the record back to show the effect. It then applies a UDF with
arguments and return value to perform another arithmetic operation on one of the
bin values, and return the integer result. It again reads the record back to
show the effect.


## Batch Examples

These examples each use multiple records to demonstrate particular API calls.


### get

	aerospike_batch_exists()
	aerospike_batch_get()

This example uses aerospike_batch_exists() to check whether a bunch of records
written to the database exist, and get their metadata (generation and TTL). It
uses aerospike_batch_get() to read all these records. It then deletes a few of
the records from the database and repeats the batch calls, showing that the
calls report the deleted records as not found, while returning all the remaining
records as before.


## Query Examples

These examples each use multiple records to demonstrate particular API calls.


### aggregate (3.0 feature)

	aerospike_index_create()
	as_query_where()
	as_query_apply()
	aerospike_query_foreach()

This example uses aerospike_index_create() to create a numeric secondary
index for a particular bin name, then using aerospike_query_foreach() it
demonstrates an aggregation query - a query which finds all records satisfying a
'where' clause (where the bin's value falls in a specified range), applies a
simple UDF (which just returns the bin's value) then aggregates these results
using a second UDF (that sums the values) before making a callback to report the
final result. The example then performs another query that accomplishes the same
task, but using different UDF internals that do an 'aggregate reduce' instead of
a 'map reduce'. The example then repeats the 'aggregate reduce' query with a UDF
filter applied before aggregation. The filter selects even numbers, showing off
filtering functionality that can't be achieved with a 'where' clause. Finally
the example performs a complex aggregation on a different (string value) bin to
show a case where the aggregation process and the value returned involve a more
complex object - a Map - rather than a simple integer.


### simple (3.0 feature)

	aerospike_index_create()
	as_query_where()
	aerospike_query_foreach()

This example uses aerospike_index_create() to create a numeric secondary
index for a particular bin name, then using aerospike_query_foreach() it
demonstrates a simple query which finds all records satisfying a 'where' clause
(where the bin's value equals a specified value) and makes a callback for each
such record, in this case a single record.


## Scan Examples

These examples each use multiple records to demonstrate particular API calls.


### background (3.0 feature)

	as_scan_apply_each()
	aerospike_scan_background()
	aerospike_scan_info()

This example uses aerospike_scan_background() to start a background scan of the
database, which applies a UDF that performs a simple arithmetic operation on a
bin's value, for every record. It then uses aerospike_scan_info() to poll and
detect when the scan is complete. Finally, it reads back all the records to show
the effect of the scan.


### standard

	aerospike_scan_foreach()

This example uses aerospike_scan_background() to do a standard foreground scan
of the database, where a callback is made for each record found.


## Async Examples

These examples demonstrate particular asynchronous API calls.

	as_event_create_loops()
	as_event_close_loops()
	aerospike_key_get_async()
	aerospike_key_put_async()
	aerospike_batch_read_async()
	aerospike_query_async()
