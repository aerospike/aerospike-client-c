Name: aerospike-client-c-devel
Version: @VERSION@
Release: 1%{?dist}
Summary: Aerospike C Client Development
License: Proprietary
Group: Development/Libraries
BuildArch: x86_64
%description
The Aerospike client is used to connect with an Aerospike server and perform database operations.
%files
%defattr(-,root,root)
/usr/include/citrusleaf
/usr/include/aerospike
/usr/lib/libaerospike.a
/usr/lib/libaerospike.so

%post
/sbin/ldconfig

