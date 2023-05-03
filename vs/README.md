# Visual Studio Environment for Aerospike C Client.

## Prerequisites

- Windows 10+
- Visual Studio 2022+

The Aerospike C client library is dependent on several third party libraries.
Since many of these libraries are difficult to build on Windows, pre-built 
third party libraries and corresponding include files are provided in the nuget
[aerospike-client-c-dependencies](https://www.nuget.org/packages/aerospike-client-c-dependencies)
package.  This package dependency is defined in the aerospike solution and should
automatically be downloaded on the first compile in Visual Studio.

### Git Submodules

This project uses git submodules, so you will need to initialize and update 
submodules before building this project.

	$ cd ..
	$ git submodule update --init

## Build Instructions

Various build targets are supported depending on the use of async database
calls and the chosen event framework.  The C client supports 64 bit targets only.

- Double click aerospike.sln
- Click one of the following solution configuration names:

	Configuration    | Usage
	---------------- | -----
	Debug            | Use synchronous methods only.
	Debug libevent   | Use libevent for async event framework.
	Debug libuv      | Use libuv for async event framework.
	Release          | Use synchronous methods only.
	Release libevent | Use libevent for async event framework.
	Release libuv    | Use libuv for async event framework (Recommended).

- Click Build -> Build Solution
