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

## Usage

Please ensure you have resolved the prerequisites and dependencies in the README.me for each module before running commands accross all modules.

### Cloning

To clone this repository, run:

	$ git clone https://github.com/aerospike/aerospike-client-c.git

This repository makes use of git submodules. Before you can build, you need to make sure all submodules are initialized and updated:

	$ cd aerospike-client-c
	$ git submodule update --init

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

