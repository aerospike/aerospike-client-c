# Citrusleaf C Shared Library

`citrusleaf-base` is a library shared by `citrusleaf-client` and `citrusleaf-libevent`. 

## Prequisites

libc and openssl packages must be installed to compile the client library.

For Debian-based distributions, execute:

    $ sudo apt-get install libc6-dev libssl-dev

For redhat derived distributions such as Fedora Core and Centos, excute:

    $ sudo yum install openssl-devel glibc-devel

Installation of these packages will also install gcc. gcc -version must show a version of 4.1 or better. g++ is also supported with the same version restriction.

## Usage

### Build

Before building, please ensure you have the prerequisites installed.

To build:

	$ make

This will generate the following files:

- `target/{target}/lib/libcf-client.so` – dynamic shared library
- `target/{target}/lib/libcf-client.a` – static archive supporting
- `target/{target}/obj/client/*.o` – object files
- `target/{target}/lib/libcf-hooked.so` – dynamic shared library supporting custom thread hooks
- `target/{target}/lib/libcf-hooked.a` – static archive supporting custom thread hooks
- `target/{target}/obj/hooked/*.o` – object files supporting custom thread hooks
- `target/{target}/include` – header files

Static linking with the `.a` prevents you from having to install the libraries on your target platform. Dynamic linking with the `.so` avoids a client rebuild if you upgrade the client.  Choose the option that is right for you.

### Clean

To clean:

	$ make clean

This will remove all generated files.


## Project Layout

The module is structured as follows:

- `examples` - example applications using the generated libraries.
- `src` – developer maintained code for the project
- `src/include` – public header files
- `src/main` – source code for the library
- `src/test` - unit tests
- `target` – generated files
- `target/{target}` – platform specific targets
- `target/{target}/include` – public headers (API) for end users
- `target/{target}/lib` – libraries for end users
- `target/{target}/obj` – generated objects files
- `target/{target}/deps` – generated dependency files


