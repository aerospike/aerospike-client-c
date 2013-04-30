# Aerospike Client Examples

This directory contains a collection of examples of using the Aerospike client.

### Examples

- **cpp_example** - An example C++ program.
- **example** – An example of using the key-value store API.
- **exists_example** – An example of how to test for existence of a record efficiently.
- **ldt_examples** – A collection of examples of using LDT.
- **udf_examples** – A collection of examples of using UDF.
- **web_examples** (defunct)


## Build

To build all examples:

	$ make

To build a specific example:

	$ make -C {example}

## Run

To run all examples:

	$ make run
	
To build a specific example:

	$ make -C {example} run