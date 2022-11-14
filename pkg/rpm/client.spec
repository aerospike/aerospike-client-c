Name: aerospike-client-c@EVENTNAME@
Version: @VERSION@
Release: 1%{?dist}
Summary: Aerospike C Client Shared Library@EVENTDESC@
License: Proprietary
Group: Development/Libraries
BuildArch: @ARCH@
Vendor: Aerospike, Inc.

%description
The Aerospike C client is used to connect with an Aerospike server and perform database operations.

%define __spec_install_post /usr/lib/rpm/brp-compress

%files
%defattr(-,root,root)
/usr/lib/libaerospike.so

%post
/sbin/ldconfig
