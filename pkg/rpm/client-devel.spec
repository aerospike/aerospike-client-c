Name: aerospike-client-c@EVENTNAME@-devel
Version: @VERSION@
Release: 1%{?dist}
Summary: Aerospike C Client Development@EVENTDESC@
License: Proprietary
Group: Development/Libraries
BuildArch: @ARCH@
Vendor: Aerospike, Inc.

%description
The Aerospike C client is used to connect with an Aerospike server and perform database operations.

%define __spec_install_post /usr/lib/rpm/brp-compress

%files
%defattr(-,root,root)
/usr/include/aerospike
/usr/include/citrusleaf
/usr/lib/libaerospike.a

%post
/sbin/ldconfig
