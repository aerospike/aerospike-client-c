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

### Record Tests

#### put

Put a record in the database

	$ ./put <namespace> <set> <key> <name> <value>
	
#### get

Get a record new record in the database

	$ ./put <namespace> <set> <key> <name> <value>
	




	