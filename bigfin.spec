%define pkg_name bigfin
%define pkg_version 0.0.1
%define pkg_release 1

Summary: SkyRing Ceph provider
Name: %{pkg_name}
Version: %{pkg_version}
Release: %{pkg_release}%{?dist}
Source0: %{pkg_name}-%{pkg_version}.tar.gz
License: Apache-2.0
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{pkg_name}-%{pkg_version}-%{release}-buildroot
Url: https://github.com/skyrings/bigfin

BuildRequires: golang
BuildRequires: python-devel
BuildRequires: python-setuptools

Requires: skyring
Requires: salt-master >= 2015.5.5
Requires: ceph

%description
SkyRing Ceph provider

%prep
%setup -n %{pkg_name}-%{pkg_version}

%build
make build-special
make pybuild

%install
rm -rf $RPM_BUILD_ROOT
install -m 755 -d $RPM_BUILD_ROOT/%{_var}/lib/skyring/providers
install -D bigfin $RPM_BUILD_ROOT/%{_var}/lib/skyring/providers
install -m 755 -d $RPM_BUILD_ROOT/%{_sysconfdir}/skyring/providers.d/
install -D conf/ceph.conf $RPM_BUILD_ROOT/%{_sysconfdir}/skyring/providers.d/ceph.conf
install -m 755 -d $RPM_BUILD_ROOT/srv/salt/_modules
install -D backend/salt/sls/* $RPM_BUILD_ROOT/srv/salt/
install -d $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin
install -D backend/salt/python/bigfin/* $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin/
install -D salt_module/ceph.py $RPM_BUILD_ROOT/srv/salt/_modules
install -D backend/salt/python/bigfin/utils.py $RPM_BUILD_ROOT/srv/salt/_modules

%clean
rm -rf $RPM_BUILD_ROOT

%files
%{_var}/lib/skyring/providers/bigfin
%{_sysconfdir}/skyring/providers.d/ceph.conf
/srv/salt/*
%{python2_sitelib}/bigfin/*
/srv/salt/_modules/*

%changelog
* Tue Dec 22 2015 Kanagaraj Mayilsamy <kmayilsa@redhat.com> - 0.0.1-1
- Initial build
