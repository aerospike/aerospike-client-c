# Citrusleaf libevent Client Library

`citrusleaf-libevent` is a libevent-based client library (API) for interfacing with Aerospike Database.

## Prerequisites

libc and openssl packages must be installed to compile the client library.

For Debian-based distributions (Debian, Ubuntu, etc.):

	$ sudo apt-get install make libc6-dev libssl-dev

For Redhat-based distributions (RHEL, CentOS, etc.):

	$ sudo yum install make glibc-devel openssl-devel

### libevent2

Some newer Linux systems (Ubuntu 12.04) come with libevent2 already installed.

On older systems, libevent2 must be installed manually, following these steps:

1. Remove the old libevent. The libevent2 library will be backward compatible,
   so software relying on the old libevent will still work with libevent2.

		$ sudo yum remove libevent

2. Download the latest libevent2 from libevent.org, e.g. libevent-2.0.20-stable.tar.gz.

		$ wget https://github.com/downloads/libevent/libevent/libevent-2.0.20-stable.tar.gz

3. Extract, and go to the resulting directory.
   
		$ tar -xvf libevent-2.0.20-stable.tar.gz
		$ cd libevent-2.0.20-stable

4. Configure libevent2 to be installed in /usr/lib and install it. (Amazingly,
   we have found the default configuration may install in /usr/local/lib.)
		
		$ ./configure --prefix=/usr
		$ make
		$ sudo make install

5. Make sure the new library is recognized.
 
		$ sudo /sbin/ldconfig


## Usage

### Build

Before building, please ensure you have the prerequisites installed.

To build:

	$ make

This will generate the following files:

- `target/{target}/lib/libev2citrusleaf.so` – dynamic shared library 
- `target/{target}/lib/libev2citrusleaf.a` – static archive
- `target/{target}/include` – header files

Static linking with the `.a` prevents you from having to install the libraries on your target platform. Dynamic linking with the `.so` avoids a client rebuild if you upgrade the client.  Choose the option that is right for you.

### Clean

To clean:

	$ make clean

This will remove all generated files.



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
