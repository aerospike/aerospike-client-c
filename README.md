# Aerospike C Client API

This repository is a collection of C client libraries for interfacing with Aerospike database clusters.

## Modules

The C Client repository is composed of multiple modules. Each module is either a client library or shared module.

- **[aerospike](./aerospike)** – Aerospike (3.x) C API 
- **[citrusleaf-base](./citrusleaf-base)** – Shared files for citrusleaf submodules.
- **[citrusleaf-client](./citrusleaf-client)** – Citrusleaf (2.x) C API (*deprecated*)
- **[citrusleaf-libevent](./citrusleaf-client)** – libevent-based API

Each module has its own `README.md` and `Makefile`. 

Please read the `README.md` for each module before using them or running make. The document contains information on prerequisites, usage and directory structure.

### [aerospike](./aerospike)

The `aerospike` module generates the following libraries:

	aerospike/target/{target}/lib/libcitrusleaf.a
	aerospike/target/{target}/lib/libcitrusleaf.so

It also generates public header files in:

	aerospike/target/{target}/include

### [citrusleaf-libevent](./citrusleaf-client)

The `citrusleaf-libevent` module generates the following libraries:

	citrusleaf-libevent/target/{target}/lib/libev2citrusleaf.a
	citrusleaf-libevent/target/{target}/lib/libev2citrusleaf.so

It also generates public header files in:

	citrusleaf-libevent/target/{target}/include

### [citrusleaf-client](./citrusleaf-client)

**NOTE:** *The `citrusleaf` libraries are deprecated in favor of `aerospike` libraries. We maintain the `citrusleaf` libraries for existing customers and ask all new users to use `aerospike` libraries.*

The `citrusleaf-client` module generates the following libraries:

	citrusleaf-client/target/{target}/lib/libcitrusleaf.a
	citrusleaf-client/target/{target}/lib/libcitrusleaf.so
	
It also generates public header files in:

	citrusleaf-client/target/{target}/include


## Usage

Please ensure you have resolved the prerequisites and dependencies in the README.me for each module before running commands accross all modules.

### Build

To build all modules:

	$ make

To build a specific module:

	$ make -C {module}

### Clean

To clean all modules:

	$ make clean

To clean a specific module:

	$ make -C {module} clean

### Other Targets

To run `{target}` on all module:

	$ make {target}

To run `{target}` on a specific module:

	$ make -C {module} {target}

