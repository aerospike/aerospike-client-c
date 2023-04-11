# Aerospike C Client Package

This package contains Aerospike C client library installers for development
and runtime.

## Contents

* aerospike-client-c[-<eventlib>]-<version>

  Runtime shared library.

* aerospike-client-c[-<eventlib>]-devel-<version>

  Development static library and header files.
      
## Prerequisites

The C client library requires other third-party libraries depending on your platform.

### Debian and Ubuntu

  $ sudo apt-get install libc6-dev libssl-dev autoconf automake libtool g++

  [Also do on Ubuntu:]
  $ sudo apt-get install ncurses-dev

  [Optional:]
  $ sudo apt-get install liblua5.1-dev

### Red Hat Enterprise Linux or CentOS

  $ sudo yum install openssl-devel glibc-devel autoconf automake libtool

  [Optional:]
  $ sudo yum install lua-devel
  $ sudo yum install gcc-c++ graphviz rpm-build 

### Fedora

  $ sudo yum install openssl-devel glibc-devel autoconf automake libtool

  [Optional:]
  $ sudo yum install compat-lua-devel-5.1.5
  $ sudo yum install gcc-c++ graphviz rpm-build 

### MacOS

* [XCode](https://itunes.apple.com/us/app/xcode/id497799835)
* [Brew Package Manager](http://brew.sh)

### Event Library (Optional)

An event library is required when C client asynchronous functionality is used.
On Linux and MacOS, the event library must be installed independently of the C client.
Install one of the supported event libraries:

#### [libuv 1.8.0+](http://docs.libuv.org) 

libuv has excellent performance and supports all platforms.  The client does not
support async TLS (SSL) sockets when using libuv.

#### [libev 4.24+](http://dist.schmorp.de/libev)

libev has excellent performance on Linux/MacOS, but its Windows implementation
is suboptimal.  Therefore, the C client supports libev on Linux/MacOS only.
The client does support async TLS (SSL) sockets when using libev.

#### [libevent 2.0.22+](http://libevent.org)

libevent is less performant than the other two options, but it does support all
platforms.  The client also supports async TLS (SSL) sockets when using libevent.

#### Event Library Notes

Event libraries usually install into /usr/local/lib on Linux/MacOS.  Most
operating systems do not search /usr/local/lib by default.  Therefore, the
following `LD_LIBRARY_PATH` setting may be necessary.

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

When compiling your async applications with aerospike header files, the event library
must be defined (`-DAS_USE_LIBEV`, `-DAS_USE_LIBUV` or `-DAS_USE_LIBEVENT`) on the
command line or in an IDE.  Example:

  $ gcc -DAS_USE_LIBEV -o myapp myapp.c -laerospike -lev -lssl -lcrypto -lpthread -lm -lz

### Installation instructions

Only one of the installers is needed on each client machine.  All installers install the libaerospike library with the same name, but different implementation.

#### Centos/RHEL
    sudo rpm -i aerospike-client-c[-<eventlib>][-devel]-<version>-<os>.<arch>.rpm
  
#### Debian/Ubuntu
    sudo dpkg -i aerospike-client-c[-<eventlib>][-devel]_<version>-<os>_<arch>.deb

#### MacOS
    open aerospike-client-c[-<eventlib>][-devel]-<version>.pkg
