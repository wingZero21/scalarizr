#
# Spec file for scalarizr packages
#
# author: Marat Komarov <marat@scalr.net>
#

%define pkgname scalarizr

Summary:        Scalarizr converts any server to Scalr-manageable node
Name:           %{pkgname}-base
Version: 0.7.64
Release:        1%{?dist}
Source0:        %{pkgname}-%{version}.tar.gz
Group:          Applications/Internet
License:        GPLv3
URL:            http://scalr.net

%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26 python26-m2crypto >= 0.20 python26-pexpect >= 2.3
Requires:       python26-pysnmp >= 4.1 python26-pyasn1 >= 0.1.7 python26-pysnmp-mibs >= 0.0.8a 
Requires:       python26-prettytable python26-PyYAML
#Requires:		python26-pymongo
Requires:		python26-pymysql
Requires:		python26-pychef
Requires:       yum-downloadonly
Requires:		yum-priorities
%else
# setuptools from pip used instead of rpm
# BuildRequires:  python-setuptools
Requires:       python >= 2.5 m2crypto >= 0.20 pexpect >= 2.3
# snmp
Requires:       pysnmp >= 4.2.4 python-pyasn1 >= 0.1.7 python-pysnmp-mibs >= 0.0.8a 
# szradm
Requires:       python-prettytable PyYAML
# mongodb behavior
#Requires:		pymongo >= 2.1
#Requires:		python-bson >= 2.1
#Requires:		python-pymongo
Requires:		python-pymysql
Requires:		python-pychef
Requires:       yum-plugin-downloadonly
Requires:       yum-plugin-priorities
%endif
Requires:		which
Requires:		e2fsprogs
Requires:       rsync >= 2.6.8
Requires:       tar
Obsoletes:      scalr-upd-client

BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-buildroot


%description
Scalarizr converts any server to Scalr-manageable node


%package -n scalarizr-ec2
Summary:        Scalarizr EC2 edition
Group:          Applications/Internet
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26-boto >= 2.13.0
%else
Requires:       python-boto >= 2.13.0
%endif
Requires:       scalarizr-base = %{version}-%{release}
Provides:       scalarizr
Obsoletes:      scalarizr < 0.7
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-nimbula
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-cloudstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud
Conflicts:		scalarizr-idcf


%description -n scalarizr-ec2
Scalarizr converts any server to Scalr-manageable node


%package -n scalarizr-eucalyptus
Summary:        Scalarizr Eucalyptus edition
Group:          Applications/Internet
Requires:		scalarizr-ec2 = %{version}-%{release}
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26-euca2ools
%else
Requires:       euca2ools >= 3.0.2
%endif

%description -n scalarizr-eucalyptus
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-eucalyptus
set -x

sed -i 's/platform = ec2/platform = eucalyptus/i' /etc/scalr/public.d/config.ini

%package -n scalarizr-rackspace
Summary:        Scalarizr Rackspace edition
Group:          Applications/Internet
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26-cloudfiles >= 1.5.1 python26-cloudservers >= 1.0
Requires:       python26-swiftclient >= 1.2.0
%else
Requires:       python-cloudfiles >= 1.5.1 python-cloudservers >= 1.0 python-httplib2
Requires:       python-swiftclient >= 1.2.0
%endif
Requires:       scalarizr-base = %{version}-%{release}
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:		scalarizr-nimbula
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-cloudstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud
Conflicts:		scalarizr-idcf

%description -n scalarizr-rackspace
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-rackspace
set -x

sed -i 's/platform = ec2/platform = rackspace/i' /etc/scalr/public.d/config.ini


%package -n scalarizr-openstack
Summary:        Scalarizr OpenStack edition
Group:          Applications/Internet
Requires:       scalarizr-base = %{version}-%{release}
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:		python26-novaclient >= 2.15.0
Requires:       python26-rackspace-novaclient >= 1.0
Requires:       python26-cinderclient >= 1.0.5
Requires:       python26-swiftclient >= 1.2.0
Requires:       python26-keystoneclient >= 0.3.2
%else
Requires:		python-novaclient >= 2.15.0
Requires:       python-rackspace-novaclient >= 1.0
Requires:       python-cinderclient >= 1.0.5
Requires:       python-swiftclient >= 1.2.0
Requires:       python-keystoneclient >= 0.3.2
%endif
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-nimbula
Conflicts:		scalarizr-cloudstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud
Conflicts:		scalarizr-idcf

%description -n scalarizr-openstack
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-openstack
set -x

sed -i 's/platform = ec2/platform = openstack/i' /etc/scalr/public.d/config.ini

%package -n scalarizr-cloudstack
Summary:        Scalarizr CloudStack (cloud.com) edition
Group:          Applications/Internet
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26-cloudstack >= 0.2.3
%else
Requires:       python-cloudstack >= 0.2.3
Requires:       lsscsi
%endif
Requires:       scalarizr-base = %{version}-%{release}
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-nimbula
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud
Conflicts:		scalarizr-idcf

%description -n scalarizr-cloudstack
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-cloudstack
set -x

sed -i 's/platform = ec2/platform = cloudstack/i' /etc/scalr/public.d/config.ini

%package -n scalarizr-ucloud
Summary:        Scalarizr uCloud (Korea Telecom) edition
Group:          Applications/Internet
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26-cloudstack
%else
Requires:       python-cloudstack
%endif
Requires:       scalarizr-base = %{version}-%{release}
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-nimbula
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-idcf

%description -n scalarizr-ucloud
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-ucloud
set -x

sed -i 's/platform = ec2/platform = ucloud/i' /etc/scalr/public.d/config.ini

%package -n scalarizr-idcf
Summary:        Scalarizr IDCF edition
Group:          Applications/Internet
%if 0%{?rhel} >= 4 && 0%{?rhel} <= 5
Requires:       python26-cloudstack
%else
Requires:       python-cloudstack
%endif
Requires:       scalarizr-base = %{version}-%{release}
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-nimbula
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud

%description -n scalarizr-idcf
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-idcf
set -x

sed -i 's/platform = ec2/platform = idcf/i' /etc/scalr/public.d/config.ini

%package -n scalarizr-nimbula
Summary:        Scalarizr Nimbula edition
Group:          Applications/Internet
Requires:       scalarizr-base = %{version}-%{release}
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-cloudstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud
Conflicts:		scalarizr-idcf

%description -n scalarizr-nimbula
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-nimbula
set -x

sed -i 's/platform = ec2/platform = nimbula/i' /etc/scalr/public.d/config.ini

%package -n scalarizr-gce
Summary:        Scalarizr Google Compute Engine edition
Group:          Applications/Internet
Requires:       scalarizr-base = %{version}-%{release}
Requires:       pyOpenSSL >= 0.13 python-httplib2
Requires:       python-google-api-client
Provides:       scalarizr
Conflicts:      scalarizr-ec2
Conflicts:      scalarizr-rackspace
Conflicts:		scalarizr-openstack
Conflicts:		scalarizr-cloudstack
Conflicts:		scalarizr-gce
Conflicts:		scalarizr-ucloud
Conflicts:		scalarizr-idcf

%description -n scalarizr-gce
Scalarizr converts any server to Scalr-manageable node

%post -n scalarizr-gce
set -x

sed -i 's/platform = ec2/platform = gce/i' /etc/scalr/public.d/config.ini



%prep
%setup -n scalarizr-%{version}


%build
%{__python} setup.py build


%pre
set -x 

%post
set -x

pub_cnf_dir='/etc/scalr/public.d'
priv_cnf_dir='/etc/scalr/private.d'
szr_version_file='/tmp/.szr-version'

installed_version=$(rpm -q --queryformat='%{VERSION}-%{RELEASE}' scalarizr-base) || installed_version=$(rpm -q --queryformat='%{VERSION}' scalarizr) || installed_version=''
installed_version=$(echo $installed_version | cut -d '.' -f 1-3)
echo -n $installed_version > $szr_version_file

compare_versions () {
        local cmp=$([ "$2" = "gt" ] && echo -n ">" || echo -n "<")
        local ret=$(%{__python} - <<python-command
def parseint(x):
	try:
		return int(x[1:] if x.startswith('r') else x)
	except ValueError:
		return x

versions = ("${1}", "${3}");
v1 = map(parseint, versions[0].replace('-', '.').split('.'));
v2 = map(parseint, versions[1].replace('-', '.').split('.'));
print int(not v1 ${cmp} v2)
python-command
)
        return $ret
}

# Cleanup 
rm -f $pub_cnf_dir/*.rpmnew


if compare_versions "$installed_version" lt '0.9.r3691-1'; then
	/sbin/iptables -I INPUT 1 -p tcp --dport 8008 -j ACCEPT
	/sbin/iptables -I INPUT 1 -p tcp --dport 8010 -j ACCEPT
fi

if compare_versions "$installed_version" lt '0.9.r3746-1'; then
	[ ! -f "$pub_cnf_dir/percona.ini" ] && ln -s "$pub_cnf_dir/mysql2.ini" "$pub_cnf_dir/percona.ini"
fi

if compare_versions "$installed_version" lt '0.9.r4762-1'; then
	dbfile="$priv_cnf_dir/db.sqlite"
	if [ -e "$dbfile" ]; then
		%{__python} - <<-EOF
			import sqlite3
			import os
			conn = sqlite3.Connection('${dbfile}')
			cur = conn.cursor()
			cur.execute('pragma table_info(p2p_message)')
			if not any(filter(lambda row: row[1] == 'format', cur.fetchall())):
			    cur.execute("alter table p2p_message add column format TEXT default 'xml'")
			    conn.commit()
			cur.close()
		EOF
	fi
fi

if compare_versions "$installed_version" lt '2.7.7'; then
	if [ -f "$priv_cnf_dir/.state" ] && [ $(cat "$priv_cnf_dir/.state") = 'running' ]; then
    	# scalr-upd-client binary here still points to old python module
		%{__python} -m scalarizr.updclient.app --make-status-file
	fi
fi

sync
umount -l "$priv_cnf_dir" 2>&1 || :

cp /usr/share/scalr/szradm.bash_completion /etc/bash_completion.d/szradm

pushd .
cd $pub_cnf_dir
if [ -f cloudfoundry.ini ]; then
	for name in cf_router.ini cf_cloud_controller.ini \
				cf_health_manager.ini cf_dea.ini cf_service.ini; do
		[ ! -f $name ] && ln -s cloudfoundry.ini $name
	done
fi
rm -f percona.ini  # Measly config in several builds 
[ ! -f percona.ini ] && ln -s mysql2.ini percona.ini
[ ! -f mariadb.ini ] && ln -s mysql2.ini mariadb.ini
[ ! -f idcf.ini ] && ln -s cloudstack.ini idcf.ini
[ ! -f ucloud.ini ] && ln -s cloudstack.ini ucloud.ini
popd


%posttrans
set -x

/sbin/chkconfig --add scalarizr
/sbin/chkconfig --add scalr-upd-client
chmod +x /etc/init.d/scalarizr
chmod +x /etc/init.d/scalr-upd-client

/sbin/service scalarizr condrestart > /dev/null 2>&1 || :

%preun
set -x

pub_cnf_dir='/etc/scalr/public.d'
priv_cnf_dir='/etc/scalr/private.d'
szr_version_file='/tmp/.szr-version'

if [ $1 = 0 ]; then
	/sbin/service scalarizr stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del scalarizr
	/sbin/chkconfig --del scalr-upd-client
	grep $priv_cnf_dir /proc/mounts > /dev/null && umount $priv_cnf_dir || :
fi


%postun
set -x

pub_cnf_dir='/etc/scalr/public.d'
priv_cnf_dir='/etc/scalr/private.d'
szr_version_file='/tmp/.szr-version'

rm -f $szr_version_file


%install
%{__python} setup.py install --root="$RPM_BUILD_ROOT"
rm -f %{buildroot}/etc/scalr/private.d/*
chmod 775 %{buildroot}/etc/scalr/private.d
mkdir -p "%{buildroot}%{_initrddir}"
cp "%{_sourcedir}/scalarizr.init" "%{buildroot}%{_initrddir}/scalarizr"
cp "%{_sourcedir}/scalr-upd-client.init" "%{buildroot}%{_initrddir}/scalr-upd-client"


%clean
rm -rf "$RPM_BUILD_ROOT"


%files
%defattr(-,root,root)
/usr
%config	           %{_initrddir}/scalarizr
%config            %{_initrddir}/scalr-upd-client
%config(noreplace) %{_sysconfdir}/scalr/public.d/*


%files -n scalarizr-ec2
%defattr(-,root,root)

%files -n scalarizr-eucalyptus
%defattr(-,root,root)

%files -n scalarizr-rackspace
%defattr(-,root,root)

%files -n scalarizr-nimbula
%defattr(-,root,root)

%files -n scalarizr-openstack
%defattr(-,root,root)

%files -n scalarizr-cloudstack
%defattr(-,root,root)

%files -n scalarizr-gce
%defattr(-,root,root)

%files -n scalarizr-ucloud
%defattr(-,root,root)

%files -n scalarizr-idcf
%defattr(-,root,root)

%changelog

