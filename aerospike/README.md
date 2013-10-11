# Aerospike C-client API

`aerospike` is a C client library (API) for interfacing with the Aerospike Database.

## Prerequisites

libc and openssl packages must be installed to compile the client library.

### Debian-based Distributions

For Debian-based distributions (Debian, Ubuntu, etc.):

	$ sudo apt-get install libc6-dev libssl-dev liblua5.1-dev autoconf automake libtool g++
	$ export CPATH=$CPATH:/usr/include/lua5.1

For Debian:

	$ sudo ln -s /usr/lib/liblua5.1.so /usr/lib/liblua.so
	$ sudo ln -s /usr/lib/liblua5.1.a /usr/lib/liblua.a

For Ubuntu:

	$ sudo ln -s /usr/lib/x86_64-linux-gnu/liblua5.1.so /usr/lib/liblua.so
	$ sudo ln -s /usr/lib/x86_64-linux-gnu/liblua5.1.a /usr/lib/liblua.a

### Redhat-based Distributions

For Redhat-based distributions (RHEL, CentOS, etc.):

	$ sudo yum install openssl-devel glibc-devel lua-devel autoconf automake libtool

Installation of these packages will also install gcc. gcc -version must show a version of 4.1 or better. g++ is also supported with the same version restriction.

## Usage

### Build

Before building, please ensure you have the prerequisites installed.

This project uses git submodules, so you will need to initialize and update the submodules before building thie project. To initialize and update submodules, run:

	$ cd ..
	$ git submodule update --init
	$ cd aerospike

To build:

	$ make

This will generate the following files:

- `target/{target}/lib/libaerospike.so` – dynamic shared library 
- `target/{target}/lib/libaerospike.a` – static archive
- `target/{target}/include` – header files

Static linking with the `.a` prevents you from having to install the libraries on your target platform. Dynamic linking with the `.so` avoids a client rebuild if you upgrade the client.  Choose the option that is right for you.

### Clean

To clean:

	$ make clean

This will remove all generated files.

### Test

To run unit tests:

	$ make test


## Project Layout

The module is structured as follows:

- `demo` - demo applications, making use of multiple features.
- `examples` - simple example applications, focused on specific features.
- `src` – developer maintained code for the project
- `src/include` – public header files
- `src/main` – source code for the library
- `src/test` - unit tests
- `target` – generated files
- `target/{target}` – platform specific targets
- `target/{target}/include` – public headers (API) for end users
- `target/{target}/lib` – libraries for end users
- `target/{target}/obj` – generated objects files
- `target/{target}/deps` – generated dependency files


