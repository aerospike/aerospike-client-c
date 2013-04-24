# Aerospike C-client API

`aerospike` is a C client library (API) for interfacing with the Aerospike Database.

## Prerequisites

libc and openssl packages must be installed to compile the client library.

For Debian-based distributions (Debian, Ubuntu, etc.):

	$ sudo apt-get install libc6-dev libssl-dev

For Redhat-based distributions (RHEL, CentOS, etc.):

	$ sudo yum install openssl-devel glibc-devel

Installation of these packages will also install gcc. gcc -version must show a version of 4.1 or better. g++ is also supported with the same version restriction.

### Library Dependencies

#### msgpack-0.5.7

Aerospike utilizes msgpack for serializing some data. You can download it here:

	http://msgpack.org/releases/cpp/msgpack-0.5.7.tar.gz 

Just extract the archive, then set the variable MSGPACK_PATH to point to the path of the extracted archive:

	$ export MSGPACK_PATH=~/msgpack-0.5.7
 

## Usage

### Build

Before building, please ensure you have the prerequisites installed.

This project uses "git submodules", so you will need to initialize and update the submodules before building thie project.

To build:

	$ make

This will generate the following files:

- `target/{target}/lib/libcitrusleaf.so` – dynamic shared library 
- `target/{target}/lib/libcitrusleaf.a` – static archive
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

- `examples` - example applications using the generated libraries.
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


