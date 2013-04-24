# Aerospike C Client API

This repository is a collection of C client libraries for interfacing with Aerospike database clusters. The repository is composed of multiple submodules.

## Submodules

The repository is composed of the following submodules:

- **aerospike** – Aerospike (3.x) C API 
- **citrusleaf-base** – Shared files for citrusleaf submodules.
- **citrusleaf-client** – Citrusleaf (2.x) C API (*deprecated*)
- **citrusleaf-libevent** – libevent-based API

Each submodule has its own `README.md` and `Makefile`. 

Please read the `README.md` for each submodule before using them. The document contains information on prerequisites, usage and directory structure.

### aerospike

The `aerospike` submodule generates the following libraries:

	aerospike/target/{target}/lib/libcitrusleaf.a
	aerospike/target/{target}/lib/libcitrusleaf.so

It also generates public header files in:

	aerospike/target/{target}/include

### citrusleaf-libevent

The `citrusleaf-libevent` submodule generates the following libraries:

	citrusleaf-libevent/target/{target}/lib/libev2citrusleaf.a
	citrusleaf-libevent/target/{target}/lib/libev2citrusleaf.so

It also generates public header files in:

	citrusleaf-libevent/target/{target}/include

### citrusleaf-client

**NOTE:** *The `citrusleaf` libraries are deprecated in favor of `aerospike` libraries. We maintain the `citrusleaf` libraries for existing customers and ask all new users to use `aerospike` libraries.*

The `citrusleaf-client` submodule generates the following libraries:

	citrusleaf-client/target/{target}/lib/libcitrusleaf.a
	citrusleaf-client/target/{target}/lib/libcitrusleaf.so
	
It also generates public header files in:

	citrusleaf-client/target/{target}/include


## Usage

### Build

To build all submodules:

	$ make

To build a specific submodule:

	$ make -C {submodule}

### Clean

To clean all submodules:

	$ make clean

To clean a specific submodule:

	$ make -C {submodule} clean

### Other Targets

To run `{target}` on all submodules:

	$ make {target}

To run `{target}` on a specific submodule:

	$ make -C {submodule} {target}

