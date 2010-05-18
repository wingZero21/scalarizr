%define name scalarizr
%define version 0.5
%define release 1

%if ! (0%{?fedora} > 12 || 0%{?rhel} > 5)
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}
%endif


Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{version}.tar.gz
Summary:        MoinMoin Wiki engine

Group:          Applications/Internet
License:        GPLv3
URL:            https://scalr.net
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildArchitectures: noarch
BuildRequires:  python-devel
Requires:       python >= 2.5
#Requires:		m2crypto >= 0.20

%description

A WikiWikiWeb is a collaborative hypertext environment, with an
emphasis on easy access to and modification of information. MoinMoin
is a Python WikiClone that allows you to easily set up your own wiki,
only requiring a Python installation. 

%prep
%setup
echo $RPM_BUILD_ROOT

%build
%{__python} setup.py build

%install
%{__python} setup.py install --root=$RPM_BUILD_ROOT

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr


%changelog
* Sun Feb 15 2009 Thomas Waldmann
- Raised requirement to Python 2.4
- Removed references to Python 1.5
- Fixed doc files.

* Thu Jun  8 2006 Johannes Poehlmann
- Fix RPM build errror "Files listet twice" 
  Replaced files list and just package all of /usr.

* Fri Mar 05 2004 Florian Festi
- Initial RPM release.
