Name: aerospike-client-c@EVENTNAME@-devel
Version: @VERSION@
Release: 1%{?dist}
Summary: Aerospike C Client Development@EVENTDESC@
License: Proprietary
Group: Development/Libraries
BuildArch: x86_64
Vendor: Aerospike, Inc.

%description
The Aerospike C client is used to connect with an Aerospike server and perform database operations.

%define __spec_install_post /usr/lib/rpm/brp-compress

%files
%defattr(-,root,root)
/usr/include/aerospike
/usr/include/citrusleaf
/usr/lib/libaerospike.a
/usr/lib/libaerospike.so
%defattr(-,aerospike,aerospike)
/opt/aerospike/client

%pre
if ! id -g aerospike >/dev/null 2>&1; then
	echo "Adding group aerospike"
	/usr/sbin/groupadd -r aerospike
fi
if ! id -u aerospike >/dev/null 2>&1; then
	echo "Adding user aerospike"
	/usr/sbin/useradd -d /opt/aerospike -c 'Aerospike server' -g aerospike aerospike
fi

%post
/sbin/ldconfig
