# Aerospike C-client API

C client API for the Aerospike Database.

## Dependencies

The following are prerequisites required before you can successfully build a C client application. 

### Linux Dependencies

#### Redhat Dependencies

Redhat based Linux Distributions (Redhat, Fedora, CentOS, SUS, etc.)require the following packages:

* `libc6-dev`
* `libssl-dev`
* `lua-devel.x86_64` - should install development resources for `lua-5.1.4` 

If `yum` is your package manager, then you should be able to run the following command:

    $ sudo yum install openssl-devel glibc-devel lua-devel.x86_64

#### Debian Dependencies

Debian based Linux Distributions (Debian, Ubuntu, etc.) require the following packages:

* `libc6-dev`
* `libssl-dev`
* `liblua5.1-dev` - should install development resources for `lua-5.1.4` 

If `apt-get` is your package manager, then you should be able to run the following command:

	$ sudo apt-get install libc6-dev libssl-dev liblua5.1-dev

***NOTE:*** Provided package name for debian apt-get

### Library Dependencies

#### msgpack-0.5.7

Aerospike utilizes msgpack for serializing some data. We recommend you follow the instructions provided on the msgpacks's [QuickStart for C Language](http://wiki.msgpack.org/display/MSGPACK/QuickStart+for+C+Language).

## Build

To build libraries:

	$ make all

To build a static archive `libcitrusleaf.a`:

	$ make libcitrusleaf.a

To build a dynamic library `libcitrusleaf.so`:

	$ make libcitrusleaf.so

To build test applications:

	$ make test

## Install

To install libraries:

	$ make install

## Testing

You can run any of the test applications in the test directory: `target/(ARCH)/bin/test`

To build tests:

	$ make test

Or a specific test in `src/test`:

	$ make <filename-without-extension>

Tests require the following dependencies installed:

* [Jansson](http://www.digip.org/jansson/) – JSON parsing library

### Record Tests

#### put

Store an object in the database

	$ ./put <namespace> <set> <key> <object>

The `<object>` should be formatted as a JSON object. Each field of the object maps to a bin in the database.

Example:

	$ ./put test demo 1 '{"name": "Bob", "age": 30, "children": ["Billy", "Barry"]}'

#### get

Retrieve an object from the database

	$ ./get <namespace> <set> <key>

The output will a formatted as a JSON object. Each field of the object maps to a bin in the database.

Example:

	$ ./get test demo 1
	{"name": "Bob", "age": 30, "children": ["Billy", "Barry"]}

#### exists

For the existence of an object in the database

	$ ./exists <namespace> <set> <key>

Example:

	$ ./exists test demo 1
	$ echo $?
	0

#### remove

Remove an object from the database

	$ ./remove <namespace> <set> <key>

Example:

	$ ./remove test demo 1
	$ ./exists test demo 1
	$ echo $?
	2




### UDF Tests

#### udf-list

List UDF files loaded on the server/cluster.

	$ ./udf-list

The output will contain new line separated list of filenames.

Example: assuming the server already contains `a_udf.lua`

	$ ./udf-list
	a_udf.lua

#### udf-put

To upload a UDF file to the server/cluster.

	$ ./udf-put <filepath>

Example:

	$ ./udf-put ~/another_udf.lua
	$ ./udf-list
	a_udf.lua
	another_udf.lua

#### udf-get

Send to stdout the contents of the file on the server/cluster.

	$ ./udf-get <filename>
	
Example:

	$ ./udf-get another_udf.lua
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

	$ ./udf-remove another_udf.lua
	# ./udf-list
	a_udf.lua

#### udf-record-apply

Apply a UDF function to a record.

	$ ./udf-record-apply <namespace> <set> <key> <filename> <function> [args …]

`args` can be one of the following types:

* JSON Array - a string containing a valid JSON Array, will be converted to a List.
* JSON Object - a string containing a valid JSON Object, will be converted to a Map.
* Integer - a string only containing an integer.
* String - any other value.

Each `arg` can be quoted to ensure it is properly captured from the argument list.

Example:

	$ ./udf-record-apply test demo 1 another_udf lappend "[1,2,3,4]" 5 6 7
	SUCCESS: List(1,2,3,4,5,6,7)
