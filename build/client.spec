Name: aerospike-client-c
Version: @VERSION@
Release: 1%{?dist}
Summary: Aerospike C Client
License: Proprietary
Group: Development/Libraries
BuildArch: x86_64
%description
The Aerospike client is used to connect with an Aerospike server and perform database operations.
%files
%defattr(-,root,root)
/usr/include/citrusleaf
/usr/include/aerospike
/usr/lib/libcitrusleaf.a
/usr/lib/libcitrusleaf.so

