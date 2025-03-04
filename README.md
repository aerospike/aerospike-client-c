# Aerospike C Client

The Aerospike C client provides a C interface for interacting with the 
[Aerospike](http://aerospike.com) Database.  The client can be built on
64-bit distributions of Linux, MacOS or Windows. Unit tests and examples
are also included.

## Build Prerequisites

### Debian and Ubuntu

	$ sudo apt-get install libc6-dev libssl-dev autoconf automake libtool g++ zlib1g-dev

	[Also do on Ubuntu:]
	$ sudo apt-get install ncurses-dev

### Red Hat Enterprise Linux, CentOS and Amazon Linux

	$ sudo yum install openssl-devel glibc-devel autoconf automake libtool libz-devel

	[Optional:]
	$ sudo yum install gcc-c++ graphviz rpm-build 

### Fedora

	$ sudo yum install openssl-devel glibc-devel autoconf automake libtool libz-devel

	[Optional:]
	$ sudo yum install gcc-c++ graphviz rpm-build 

### MacOS

* [XCode](https://itunes.apple.com/us/app/xcode/id497799835)
* [Brew Package Manager](http://brew.sh)

Run this script after installing XCode and Brew:

	$ xcode/prepare_xcode

### Windows

See [Windows Build](vs).
	
### OpenSSL Library

This minimum OpenSSL library version is 1.0.2.

### Event Library (Optional)

An event library is required when C client asynchronous functionality is used.
On Linux and MacOS, the event library must be installed independently of the C client.
Install one of the supported event libraries:

#### [libuv 1.15.0+](http://docs.libuv.org) 

libuv has excellent performance and supports all platforms.  If using libuv and TLS (SSL),
OpenSSL 1.1.0 or greater is required.  Use `install_libuv` to install on Linux/MacOS.
See [Windows Build](vs) for libuv configuration on Windows.

#### [libev 4.24+](http://dist.schmorp.de/libev)

libev has excellent performance on Linux/MacOS, but its Windows implementation
is suboptimal.  Therefore, the C client supports libev on Linux/MacOS only.
The client does support async TLS (SSL) sockets when using libev.  Use 
`install_libev` to install.

#### [libevent 2.1.8+](http://libevent.org)

libevent is less performant than the other two options, but it does support all
platforms.  The client also supports async TLS (SSL) sockets when using libevent.
Use `install_libevent` to install on Linux/MacOS.  See [Windows Build](vs)
for libevent configuration on Windows.

#### Event Library Notes

Event libraries usually install into /usr/local/lib on Linux/MacOS.  Most
operating systems do not search /usr/local/lib by default.  Therefore, the
following `LD_LIBRARY_PATH` setting may be necessary.

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

When compiling your async applications with aerospike header files, the event library
must be defined (`-DAS_USE_LIBUV`, `-DAS_USE_LIBEV` or `-DAS_USE_LIBEVENT`) on the command line or
in an IDE.  Example:

	$ gcc -DAS_USE_LIBUV -o myapp myapp.c -laerospike -lev -lssl -lcrypto -lpthread -lm -lz

## Build

The remaining sections are applicable to Linux/MacOS platforms.
See [Windows Build](vs) for Windows build instructions.

Before building, please ensure you have the prerequisites installed.  This project uses 
git submodules, so you will need to initialize and update submodules before building 
this project.

	$ git submodule update --init

Build default library:

	$ make [EVENT_LIB=libuv|libev|libevent]

Build examples:

	$ make
	$ make EVENT_LIB=libuv    # Support asynchronous functions with libuv
	$ make EVENT_LIB=libev    # Support asynchronous functions with libev
	$ make EVENT_LIB=libevent # Support asynchronous functions with libevent

The build adheres to the _GNU_SOURCE API level. The build will generate the following files:

- `target/{target}/include` – header files
- `target/{target}/lib/libaerospike.a` – static archive
- `target/{target}/lib/libaerospike.so` – dynamic shared library (for Linux)
  **or**
- `target/{target}/lib/libaerospike.dylib` – dynamic shared library (for MacOS)

Static linking with the `.a` prevents you from having to install the libraries on your 
target platform. Dynamic linking with the `.so` avoids a client rebuild if you upgrade 
the client. Choose the option that is right for you.

Build alias:

If always building with the same asynchronous framework, creating an alias is recommended.

	$ alias make="make EVENT_LIB=libuv"

## Clean

To clean up build products:

	$ make clean

This will remove all files in the `target` directory.

## Test

To run unit tests:

	$ make [EVENT_LIB=libuv|libev|libevent] [AS_HOST=<hostname>] test

or with valgrind:

	$ make [EVENT_LIB=libuv|libev|libevent] [AS_HOST=<hostname>] test-valgrind

## Install

To install header files and library on the current machine:

	$ sudo make install

## Package

Installer packages can be created for RedHat (rpm), Debian (deb), Mac OS X (pkg).
These packages contain C client libraries and header files. Package creation 
requires doxygen 1.8 or greater and its dependencies (including graphviz).
Doxygen is used to create online HTML documentation.

Build the client package on the current platform:

	$ make package

The generated packages are located in `target/packages`.

