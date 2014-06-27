# Aerospike C Client Library

`aerospike` is a C client library (API) for interfacing with the Aerospike Database.

## Prerequisites

The C client library requires `gcc` and `g++` 4.1 or newer.

Development packages of the following libraries must be installed.

- `libc`
- `openssl`
- `lua 5.1`

### Debian 6+ and Ubuntu 12+

For Debian-based distributions (Debian, Ubuntu, etc.):

	$ sudo apt-get install libc6-dev libssl-dev liblua5.1-dev autoconf automake libtool g++

### Redhat Enterprise Linux 6+

For Redhat Enterprise 6 or newer, or related distributions (CentOS, etc.):

	$ sudo yum install openssl-devel glibc-devel autoconf automake libtool lua-devel

### Fedora 20+

For Fedora 20 or newer:

	$ sudo yum install openssl-devel glibc-devel autoconf automake libtool compat-lua-devel-5.1.5

### Lua from source

If you wish to install Lua from source, make sure you install Lua 5.1. By default, Lua will be installed in /usr/local with the correct names, so no extra steps are necessary - only validate that liblua.so and liblua.a are in the default library search path of your build environment.

### MacOS X

For MacOS X:

	$ # Compile and install the latest Lua 5.1
	$ mkdir -p external && cd external
	$ curl -R -O http://www.lua.org/ftp/lua-5.1.5.tar.gz
	$ tar zxf lua-5.1.5.tar.gz && cd lua-5.1.5
	$ make macosx test
	$ sudo make install
	$ cd ../
	$ # Compile and install the latest OpenSSL
	$ curl -R -O https://www.openssl.org/source/openssl-1.0.1h.tar.gz
	$ tar zxvf openssl-1.0.1h.tar.gz && cd openssl-1.0.1h
	$ ./Configure darwin64-x86_64-cc
	$ make
	$ sudo make install

## Usage

### Build

Before building, please ensure you have the prerequisites installed.

This project uses git submodules, so you will need to initialize and update the submodules before building thie project. To initialize and update submodules, run:

	$ git submodule update --init

To build:

	$ make

This will generate the following files:

- `target/{target}/include` – header files
- `target/{target}/lib/libaerospike.a` – static archive
- `target/{target}/lib/libaerospike.so` – dynamic shared library (for Linux)
  **or**
- `target/{target}/lib/libaerospike.dylib` – dynamic shared library (for MacOS)

Static linking with the `.a` prevents you from having to install the libraries on your target platform. Dynamic linking with the `.so` avoids a client rebuild if you upgrade the client.  Choose the option that is right for you.

### Install

For MacOS builds of tools such as AQL you will want to also call the following,
which places the dynamic shared library in /usr/local/lib/

	$ sudo make install

### Clean

To clean:

	$ make clean

This will remove all generated files.

### Test

To run unit tests:

	$ make test


## Project Layout

The module is structured as follows:

- `benchmarks` - measure read/write performance.
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


