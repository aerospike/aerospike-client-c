## Aerospike C Client Package

This package contains Aerospike C client library installers, documentation and source code examples.

### Contents

* aerospike-client-c-<version>
	
  Aerospike C client library installer for synchronous functionality only.  

* aerospike-client-c-devel-<version>

  Aerospike C client library and header file installer for synchronous functionality only.
  This installer is used for developers when compiling/linking their applications with the 
  libaerospike static or dynamic shared library.
  
* aerospike-client-c-libev-<version>

  Aerospike C client library installer for synchronous and asynchronous functionality 
  implemented with the libev framework.  libev must be installed separately on your machine.

* aerospike-client-c-libev-devel-<version>

  Aerospike C client library and header file installer for synchronous and asynchronous functionality 
  implemented with the libev framework.  libev must be installed separately on your machine.
  This installer is used for developers when compiling/linking their applications with the 
  libaerospike static or dynamic shared library.

* aerospike-client-c-libuv-<version>

  Aerospike C client library installer for synchronous and asynchronous functionality
  implemented with the libuv framework.  libuv must be installed separately on your machine.

* aerospike-client-c-libuv-devel-<version>

  Aerospike C client library and header file installer for synchronous and asynchronous functionality
  implemented with the libuv framework.  libuv must be installed separately on your machine.
  This installer is used for developers when compiling/linking their applications with the 
  libaerospike static or dynamic shared library.

* benchmarks

  C client read/write benchmarks.

* docs

  Online C client documentation.

* examples

  C client source code examples.
      
### Prerequisites

The C client library requires other third-party libraries depending on your platform.

#### Debian 7+ and Ubuntu 12+

    $ sudo apt-get install libc6-dev libssl-dev autoconf automake libtool g++

    [Also do on Ubuntu 12+:]
    $ sudo apt-get install ncurses-dev

    [Optional:]
    $ sudo apt-get install liblua5.1-dev

#### Red Hat Enterprise Linux or CentOS 6+

    $ sudo yum install openssl-devel glibc-devel autoconf automake libtool

    [Optional:]
    $ sudo yum install lua-devel
    $ sudo yum install gcc-c++

#### Fedora 20+

    $ sudo yum install openssl-devel glibc-devel autoconf automake libtool

    [Optional:]
    $ sudo yum install compat-lua-devel-5.1.5
    $ sudo yum install gcc-c++

#### Mac OS X

Download and install [XCode](https://itunes.apple.com/us/app/xcode/id497799835).

#### libev (Optional. Used for asynchronous functions)

Download and install [libev](http://dist.schmorp.de/libev) version 4.20 or greater.

#### libuv (Optional. Used for asynchronous functions)

Download and install [libuv](http://docs.libuv.org) version 1.7.5 or greater.

libev and libuv usually install into /usr/local/lib.  Most operating systems do not 
search /usr/local/lib by default.  Therefore, the following LD_LIBRARY_PATH setting may 
be necessary.

    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib


### Installation instructions

Only one of the six installers is needed on each client machine.  All installers install the 
libaerospike library with same name, but different implementation.

#### Centos/RHEL
    sudo rpm -i aerospike-client-c[-<eventlib>][-devel]-<version>-<os>.x86_64.rpm
  
#### Debian/Ubuntu
    sudo dpkg -i aerospike-client-c[-<eventlib>][-devel]-<version>-<os>.x86_64.deb

#### Mac OS X
    open aerospike-client-c[-<eventlib>][-devel]-<version>.pkg

