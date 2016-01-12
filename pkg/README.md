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
      
### Installation instructions

    Only one of the six installers is needed on each client machine.  All installers install the 
    libaerospike library with same name, but different implementation.

    * Centos/RHEL: sudo rpm -i aerospike-client-c[-<eventlib>][-devel]-<version>-<os>.x86_64.rpm
      
    * Debian/Ubuntu: sudo dpkg -i aerospike-client-c[-<eventlib>][-devel]-<version>-<os>.x86_64.deb
    
    * Mac OS X: open aerospike-client-c[-<eventlib>][-devel]-<version>.pkg
