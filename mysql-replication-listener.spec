%define _libdir /usr/lib

Name:		mysql-replication-listener
Version:	0.0.47
Release:	9%{?dist}
Summary:	A STL/Boost based C++ library used for connecting to a MySQL server and process the replication stream as a slave.

Group:		Development/Libraries
License:	GNU GPL v2
URL:		https://launchpad.net/mysql-replication-listener
Source0:	mysql-replication-listener.tar.gz
Patch0:		mysql-replication-listener-as_c_str.patch
Patch1:		mysql-replication-listener-longlong.patch
Patch2:		mysql-replication-listener-enum.patch
Patch3:		mysql-replication-listener-date.patch
Patch4:		mysql-replication-listener-time-year.patch
Patch5:		mysql-replication-listener-decimal.patch
BuildRoot:	%(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildRequires:	gcc-c++, make, cmake, boost-devel, openssl-devel
Requires:	glibc, libstdc++, zlib, boost-devel, openssl

%description
The MySQL Replicant Library is a C++ library for reading MySQL
replication events, either by connecting to a server or by reading
from a file. To handle reading from a server, it includes a very
simple client.

%prep
%setup -q -n %{name}
%patch0
%patch1
%patch2
%patch3
%patch4
%patch5

%build
%cmake
make %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%{_includedir}/access_method_factory.h
%{_includedir}/basic_content_handler.h
%{_includedir}/basic_transaction_parser.h
%{_includedir}/binlog_api.h
%{_includedir}/binlog_driver.h
%{_includedir}/binlog_event.h
%{_includedir}/bounded_buffer.h
%{_includedir}/field_iterator.h
%{_includedir}/file_driver.h
%{_includedir}/protocol.h
%{_includedir}/resultset_iterator.h
%{_includedir}/row_of_fields.h
%{_includedir}/rowset.h
%{_includedir}/tcp_driver.h
%{_includedir}/utilities.h
%{_includedir}/value.h
%{_libdir}/libreplication.a
%{_libdir}/libreplication.so
%{_libdir}/libreplication.so.0.1
%{_libdir}/libreplication.so.1
