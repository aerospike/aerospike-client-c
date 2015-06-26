# Aerospike C Client

The Aerospike C client provides the standard C Application Programming
Interface (API) for interfacing with the [Aerospike](http://aerospike.com)
Database as well as a suite of example applications demonstrating various
features of the C client.

## Build Prerequisites

The C client can be built on most recent 64-bit Linux distributions
and Mac OS X 10.8 or greater.

Building the C client library requires `gcc` / `g++` 4.1 or newer (on
Linux) or `XCode` / `clang` (on Mac OS X.)

### Dependencies

Development packages of the following libraries must be installed.

- `libc`
- `openssl`

The C client requires [Lua](http://www.lua.org) 5.1 support for the
client-side portion of User Defined Function (UDF) query aggregation.
By default, the C client builds with Lua support provided by the
included `lua` submodule.

Optionally, Lua support may be provided by either the included `luajit`
submodule or by the build environment.

To enable [LuaJIT](http://luajit.org) 2.0.3, the build must be performed
with the `USE_LUAJIT=1` option passed on all relevant `make` command
lines (i.e., the C client itself, the benchmarks sample application, and
the API examples.) [Note that on some platforms, [Valgrind](http://www.valgrind.org)
may not function out-of-the-box on applications built with the C client
when LuaJIT is enabled without using an unreleased version of LuaJIT
built with additional options.]

To use Lua provided by the development environment, either the `lua5.1`
development package may be installed (on platforms that have it), or
else Lua 5.1.5 may be built from the source release and installed into
the standard location (usually `/usr/local/`.) In either of these two
cases, the build must be performed with the option `USE_LUAMOD=0` passed
on all relevant `make` command lines.

#### Debian 6+ and Ubuntu 12+

For Debian-based distributions (Debian, Ubuntu, etc.):

	$ sudo apt-get install libc6-dev libssl-dev autoconf automake libtool g++

	[Also do on Ubuntu 12+:]

	$ sudo apt-get install ncurses-dev

	[Optional:]

	$ sudo apt-get install liblua5.1-dev

#### Red Hat Enterprise Linux 6+

For Red Hat Enterprise 6 or newer, or related distributions (CentOS, etc.):

	$ sudo yum install openssl-devel glibc-devel autoconf automake libtool

	[Optional:]

	$ sudo yum install lua-devel

#### Fedora 20+

For Fedora 20 or newer:

	$ sudo yum install openssl-devel glibc-devel autoconf automake libtool

	[Optional:]

	$ sudo yum install compat-lua-devel-5.1.5

#### Mac OS X

For Mac OS X:

	$ # Compile and install the latest OpenSSL
	$ curl -R -O https://www.openssl.org/source/openssl-1.0.1h.tar.gz
	$ tar zxvf openssl-1.0.1h.tar.gz && cd openssl-1.0.1h
	$ ./Configure darwin64-x86_64-cc
	$ make
	$ sudo make install

	[Optional:]

	$ cd ../
	$ # Compile and install the latest Lua 5.1
	$ mkdir -p external && cd external
	$ curl -R -O http://www.lua.org/ftp/lua-5.1.5.tar.gz
	$ tar zxf lua-5.1.5.tar.gz && cd lua-5.1.5
	$ make macosx test
	$ sudo make install

## Usage

### Build

Before building, please ensure you have the prerequisites installed.

This project uses git submodules, so you will need to initialize and update the submodules before building this project. To initialize and update submodules, run:

	$ git submodule update --init

To build:

	$ make

This will generate the following files:

- `target/{target}/include` – header files
- `target/{target}/lib/libaerospike.a` – static archive
- `target/{target}/lib/libaerospike.so` – dynamic shared library (for Linux)
  **or**
- `target/{target}/lib/libaerospike.dylib` – dynamic shared library (for MacOS)

Static linking with the `.a` prevents you from having to install the libraries on your target platform. Dynamic linking with the `.so` avoids a client rebuild if you upgrade the client. Choose the option that is right for you.

### Packaging

After building, packaging options are available for making RedHat
Package Manager (RPM), ".deb", Mac OS X "Darwin", and source
distribution packages.  The results of packaging will left in the
`pkg/packages/` directory.

First, execute the following two build commands to generate the
documentation and example applications:

To build the Aerospike C client API documentation, use:

	$ make docs

To prepare the examples in a form suitable for distribution in a package, use:

	$ make examples

Next, depending upon your build platform type, execute the appropriate
one of the following three build packaging commands:

On a RedHat-based Linux distribution, to build the ".rpm" packages, use:

	$ make rpm

On a Debian-based Linux distribution, to build the ".deb" packages, use:

	$ make deb

On a Mac OS X "Darwin" system, to build the ".pkg" packages, use:

	$ make mac

*Note:* On each of the above three systems, the native packages for the
Aerospike C client library and as the Aerospike C client API software
development kit, i.e., the "devel" package, will be built, as well as a
".tgz" archive containing both of the native packages, along with the
documentation and examples.

You can also build the package for the current platform using:

	$ make package

Finally, to build the source distribution on any platform, use:

	$ make source

### Install

For running C client-based tools such as `aql` it is necessary to
install the C client as follows:

	$ sudo make install

This places the C client dynamic shared library into `/usr/local/lib/`.

### Clean

To clean up build products:

	$ make clean

This will remove all generated files.

### Test

To run unit tests:

	$ make test

or with valgrind
	$ make test-valgrind

## Project Layout

The module is structured as follows:

- `LICENSE` - Licenses for open source components.
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
- `target/apidocs` - generated API documentation files
- `pkg/packages` - generated package files
