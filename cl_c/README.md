# aerospike client-c

C client API for the Aerospike Database.

## Prerequisites

The following are prerequisites required before you can successfully build a C client application. 

### Linux Prerequisites

#### Redhat Packages

Redhat based Linux Distributions (Redhat, Fedora, CentOS, SUS, etc.)require the following packages:

* libc6-dev
* libssl-dev
* lua-5.1.4

If `yum` is your package manager, then you should be able to run the following command:

	$ sudo yum install openssl-devel glibc-devel


#### Debian Packages

Debian based Linux Distributions (Debian, Ubuntu, etc.) require the following packages:

* libc6-dev 
* libssl-dev
* lua-5.1.4

If `apt-get` is your package manager, then you should be able to run the following command:

	$ sudo apt-get install libc6-dev libssl-dev


### Other Prerequisites

#### msgpack-0.5.7

Aerospike utilizes msgpack for serializing some data. We recommend you follow the instructions provided on the msgpacks's [QuickStart for C Language](http://wiki.msgpack.org/display/MSGPACK/QuickStart+for+C+Language).
