# Ad UDF C++ Demo

This is a use case demonstration of user behavior tracking for advertising.

Key features of this demo include:
- use of C++
- use of 3.0 Client API
- applying a UDF (User Defined Function) written in Lua

## Build

Prior to building this demo, you first need the 3.0 C Client development package
installed. The development package installs the header files and libraries for
building a C/C++ application.

This demo expects the following to be installed:
- `/usr/lib/libaerospike.a`
- `/usr/include/aerospike`
- `/usr/include/citrusleaf`

These paths are defined by the `PREFIX` variable, which defaults to `/usr`.

If you installed the libraries and header files in a different location, then
you can override `PREFIX` when running make:

    [prompt] $ make PREFIX=<PATH>

If you have the client source repository, then you can set the `CLIENTREPO`
variable to the path of the repository.

    [prompt] $ make CLIENTREPO=<PATH>

## Run

To run the demo, you can simply run:

    [prompt] $ make demo

You may override the demo program's default arguments via the `OPTIONS` variable:

    [prompt] $ make demo OPTIONS="-b 10 -u 10"

To list available options, run:

    [prompt] $ make demo OPTIONS="--help"
