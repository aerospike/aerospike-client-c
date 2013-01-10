# Aerospike C-client API

C client API for the Aerospike Database.
In this README, enclosed you will find information on:
(1) Dependencies
(2) Build
(3) Tests (KV, UDF)
(4) Examples
(5) API Documentation

(1) Dependencies

The following are prerequisites required before you can successfully build a C client application. 

#### Redhat Dependencies

Redhat based Linux Distributions (Redhat, Fedora, CentOS, SUS, etc.)require the following packages:

* `libc6-dev`
* `libssl-dev`

If `yum` is your package manager, then you should be able to run the following command:

    $ sudo yum install openssl-devel glibc-devel 

#### Debian Dependencies

Debian based Linux Distributions (Debian, Ubuntu, etc.) require the following packages:

* `libc6-dev`
* `libssl-dev`

If `apt-get` is your package manager, then you should be able to run the following command:

	$ sudo apt-get install libc6-dev libssl-dev 

***NOTE:*** Provided package name for debian apt-get

### Library Dependencies

#### msgpack-0.5.7

Aerospike utilizes msgpack for serializing some data. http://msgpack.org/ (Currently latest, http://msgpack.org/releases/cpp/msgpack-0.5.7.tar.gz)

Once downloaded and unpacked, set the environment variable MSGPACK_PATH to point to the path and Aerospike build will pick it up:
export MSGPACK_PATH=~/msgpack-0.5.7
 
#### jansson

Aerospike utilizes jannson for some of the test utility.

Once downloaded and unpacked, set the environment variable JANSSON_PATH to point to the path and Aerospike build will pick it up:
export JANSSON_PATH=~/jansson-2.4

(2) Build

Make sure the two dependency environment variables MSGPACK_PATH & JANSSON_PATH are set:
 
To build libraries:

	$ make all

To build a static archive `libcitrusleaf.a`:

	$ make libcitrusleaf.a

To build a dynamic library `libcitrusleaf.so`:

	$ make libcitrusleaf.so

To build test applications:

	$ make test

To install the test utilities:

	$ make install

This will install the utilities in the "/opt/citrusleaf/bin" directory

(3) Testing

## KV Testing

#### put - Store an object in the database

	$ aerospike put <namespace> <set> <key> <object>

The `<object>` should be formatted as a JSON object. Each field of the object maps to a bin in the database.

Example:

	$ aerospike put test demo 1 '{"name": "Bob", "age": 30, "children": ["Billy", "Barry"]}'

#### get - Retrieve an object from the database

	$ aerospike get <namespace> <set> <key>

The output will a formatted as a JSON object. Each field of the object maps to a bin in the database.

Example:

	$ aerospike get test demo 1
	{"name": "Bob", "age": 30, "children": ["Billy", "Barry"]}

#### exists - For the existence of an object in the database

	$ aerospike exists <namespace> <set> <key>

Example:

	$ aerospike exists test demo 1
	$ echo $?
	0

#### remove - Remove an object from the database

	$ aerospike remove <namespace> <set> <key>

Example:

	$ aerospike remove test demo 1
	$ aerospike exists test demo 1
	$ echo $?
	2


## UDF Tests

#### udf-list

List UDF files loaded on the server/cluster.

	$ aerospike udf-list

The output will contain new line separated list of filenames.

Example: assuming the server already contains `a_udf.lua`

	$ aerospike udf-list
	a_udf.lua

#### udf-put

To upload a UDF file to the server/cluster.

	$ aerospike udf-put <filepath>

Example:

	$ aerospike udf-put ~/another_udf.lua
	$ aerospike udf-list
	a_udf.lua
	another_udf.lua

#### udf-get

Send to stdout the contents of the file on the server/cluster.

	$ aerospike udf-get <filename>
	
Example:

	$ aerospike udf-get another_udf.lua
	-- append to a list
	function lappend(r, l, ...)
        local len = select('#',...)
        for i=1, len do
            list.append(l, select(i,...))
        end
		return l
	end

#### udf-remove

Send to stdout the contents of the file on the server/cluster.

	$ aerospike udf-remove another_udf.lua
	# aerospike udf-list
	a_udf.lua

#### udf-record-apply

Apply a UDF function to a record.

	$ aerospike udf-record-apply <namespace> <set> <key> <filename> <function> [args …]

`args` can be one of the following types:

* JSON Array - a string containing a valid JSON Array, will be converted to a List.
* JSON Object - a string containing a valid JSON Object, will be converted to a Map.
* Integer - a string only containing an integer.
* String - any other value.

Each `arg` can be quoted to ensure it is properly captured from the argument list.

Example:

	$ aerospike udf-record-apply test demo 1 another_udf lappend "[1,2,3,4]" 5 6 7
	SUCCESS: List(1,2,3,4,5,6,7)

(4) Examples

## KV Example

In the "example" directory, there is a simple program for showing usage of basic "get" and "put" using the C api.
In addition, in the "tools" directory, there are "key_c" and "loop_c" programs.

## UDF Example

In the "udf_example" directory, there is a "loop_udf" program.

(5) API Documentation
In the "gendoc" directory is the detailed html API documentation

