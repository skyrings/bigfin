Summary: SKYRING ceph provider
Name: bigfin
Version: 0.0.30
Release: 1%{?dist}
Source0: %{name}-%{version}.tar.gz
License: ASL 2.0
Group: Development/Libraries
Url: https://github.com/skyrings/bigfin

BuildRequires: golang
BuildRequires: python-devel
BuildRequires: python-setuptools

Requires: skyring
Requires: salt-master >= 2015.5.5

%description
SKYRING Ceph provider

%prep
%setup

%build
make build-special
make pybuild

%install
rm -rf $RPM_BUILD_ROOT
install -m 755 -d $RPM_BUILD_ROOT/%{_var}/lib/skyring/providers
install -D bigfin $RPM_BUILD_ROOT/%{_var}/lib/skyring/providers
install -m 755 -d $RPM_BUILD_ROOT/%{_sysconfdir}/skyring/providers.d/
install -Dm 0644 conf/ceph.conf $RPM_BUILD_ROOT/%{_sysconfdir}/skyring/providers.d/ceph.conf
install -m 755 -d $RPM_BUILD_ROOT/srv/salt/_modules
install -D backend/salt/sls/* $RPM_BUILD_ROOT/srv/salt/
install -d $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin
install -D backend/salt/python/bigfin/* $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin/
install -Dm 0644 salt_module/ceph.py $RPM_BUILD_ROOT/srv/salt/_modules
install -Dm 0644 salt_module/mon_remote.py $RPM_BUILD_ROOT/srv/salt/_modules
install -Dm 0644 backend/salt/python/bigfin/utils.py $RPM_BUILD_ROOT/srv/salt/_modules
install -Dm 0644 conf/ceph.dat.sample $RPM_BUILD_ROOT/%{_sysconfdir}/skyring/providers.d/ceph.dat
install -Dm 0644 provider/ceph.evt $RPM_BUILD_ROOT/%{_sysconfdir}/skyring/providers.d/ceph.evt
chmod -x $RPM_BUILD_ROOT/srv/salt/add_ceph_mon.sls
chmod -x $RPM_BUILD_ROOT/srv/salt/prepare_ceph_osd.sls
chmod -x $RPM_BUILD_ROOT/srv/salt/start_ceph_mon.sls
chmod -x $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin/__init__.py
chmod -x $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin/saltwrapper.py
chmod -x $RPM_BUILD_ROOT/%{python2_sitelib}/bigfin/utils.py

%clean
rm -rf $RPM_BUILD_ROOT

%files
%{_var}/lib/skyring/providers/bigfin
%config(noreplace) %{_sysconfdir}/skyring/providers.d/ceph.conf
/srv/salt/*
%{python2_sitelib}/bigfin/*
/srv/salt/_modules/*
%config(noreplace) %{_sysconfdir}/skyring/providers.d/ceph.dat
%config(noreplace) %{_sysconfdir}/skyring/providers.d/ceph.evt
%doc README.md

%changelog
* Tue Dec 22 2015 Kanagaraj Mayilsamy <kmayilsa@redhat.com> - 0.0.1-1
- Initial build
