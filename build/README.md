## Aerospike C Client Package

This package contains Aerospike C client library installers, documentation and source code examples.

### Contents

    * aerospike-client-c-<version>-<os>.x86_64.{rpm,deb}
	
      Aerospike C client library installer.  This installer is only needed 
      when the dynamic libaerospike shared library is needed on target 
      platforms.  Installation instructions:

        Centos/RHEL: sudo rpm -i aerospike-client-c-<version>-<os>.x86_64.rpm
        Debian/Ubuntu: sudo dpkg -i aerospike-client-c-<version>-<os>.x86_64.deb

    * aerospike-client-c-devel-<version>.<os>.x86_64.{rpm,deb} 

      Aerospike C client library and header file installer.
      This installer is used for developers when compiling/linking their 
      applications with the libaerospike static or dynamic shared library.
      Installation instructions:

        Centos/RHEL: sudo rpm -i aerospike-client-c-devel-<version>-<os>.x86_64.rpm
        Debian/Ubuntu: sudo dpkg -i aerospike-client-c-devel-<version>-<os>.x86_64.deb

    * docs

      Online C client documentation.

    * examples

      C client source code examples.
