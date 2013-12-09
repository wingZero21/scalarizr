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
%endif
Requires:		scalr-upd-client
Requires:		which
Requires:		e2fsprogs
Requires:       rsync >= 2.6.8
Requires:       tar


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

%description -n scalarizr-eucalyptus
Scalarizr converts any server to Scalr-manageable node


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


%package -n scalarizr-gce
Summary:        Scalarizr Google Compute Engine edition
Group:          Applications/Internet
Requires:       scalarizr-base = %{version}-%{release}
Requires:       pyOpenSSL >= 0.13 python-httplib2
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



%prep
%setup -n scalarizr-%{version}


%build
%{__python} setup.py build


%pre
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

# HotFix: remove crappy files in private.d/ from old packages
umount -l "$priv_cnf_dir" 2>&1 || :
if ! grep "$priv_cnf_dir" /proc/mounts; then
	if compare_versions $installed_version lt '0.7.14'; then
		rm -f $priv_cnf_dir/*
	fi
fi
[ -f /mnt/privated.img ] && mount /mnt/privated.img "$priv_cnf_dir" -o loop

if compare_versions $installed_version lt '0.7.0'; then
	if grep 'localhost:8013' "$pub_cnf_dir/config.ini" > /dev/null; then
        	sed -i "s/localhost:8013/0.0.0.0:8013/g" "$pub_cnf_dir/config.ini"
	fi        

	# Fix 'mysql' section name
	if grep 'behaviour_mysql' "$priv_cnf_dir/mysql.ini" > /dev/null 2>&1; then
	    	sed -i 's/behaviour_mysql/mysql/1' "$priv_cnf_dir/mysql.ini"
	fi

	# Convert mysql storage configuration
	if [ -f "$priv_cnf_dir/mysql.ini" ]; then
		[ ! -d "$priv_cnf_dir/storage" ] && mkdir "$priv_cnf_dir/storage"
		grep snapshot_id "$priv_cnf_dir/mysql.ini" | sed 's/snapshot_id\s\+=\s\+\(.*\)/{ "type": "ebs", "id": "\1" }/' > "$priv_cnf_dir/storage/mysql-snap.json"
		sed -i '/snapshot_id/d' "$priv_cnf_dir/mysql.ini"
		device=$(cat /proc/mounts | grep dbstorage | awk '{ print $1 }') 
		volume_id=$(grep volume_id "$priv_cnf_dir/mysql.ini" | sed 's/volume_id\s\+=\s\+\(.*\)/\1/')
		echo '{ "type": "ebs", "id": "'$volume_id'", "device": "'$device'" }' > "$priv_cnf_dir/storage/mysql.json"
		sed -i '/volume_id/d' "$priv_cnf_dir/mysql.ini"
	fi
	
	# Add new colums
	dbfile="$priv_cnf_dir/db.sqlite"
	if [ -e "$dbfile" ]; then
				%{__python} <<EOF
import sqlite3
import os
conn = sqlite3.Connection('${dbfile}')
cur = conn.cursor()
cur.execute('pragma table_info(p2p_message)')
if not any(filter(lambda row: row[1] == 'in_consumer_id', cur.fetchall())):
	cur.execute('alter table p2p_message add column in_consumer_id TEXT')
	cur.execute('update p2p_message set in_consumer_id = "http://0.0.0.0:8013" where is_ingoing = 1')
	conn.commit()	
if not cur.execute('pragma table_info(storage)').fetchall():
	cur.execute('CREATE TABLE storage ("volume_id" TEXT, "type" TEXT, "device" TEXT, "state" TEXT)')
	conn.commit()
if os.path.exists('${priv_cnf_dir}/storage/mysql.json'):
	cur.execute('INSERT INTO storage VALUES (?, ?, ?, ?)', ('${volume_id}', 'ebs', '${device}', 'attached'))
	conn.commit()
cur.close()
EOF
	fi

	# Add new options
	if ! grep 'report_email' "$pub_cnf_dir/config.ini" > /dev/null; then
		sed -i 's/\(\[messaging\]\)/report_email = szr-report@scalr.com\n\n\1/' "$pub_cnf_dir/config.ini"
	fi
	if ! grep 'ssh_auth_keys' "$pub_cnf_dir/config.ini" > /dev/null; then
		sed -i 's/\(\[handlers\]\)/\1\nssh_auth_keys = scalarizr.handlers.ssh_auth_keys\n/' "$pub_cnf_dir/config.ini"
	fi
	if ! grep 'hostname_as_pubdns' "$pub_cnf_dir/ec2.ini" > /dev/null; then
		sed -i 's/\(\[handlers\]\)/hostname_as_pubdns = 1\n\n\1/' "$pub_cnf_dir/ec2.ini"
	fi
	if ! grep 'upstream_app_role' "$pub_cnf_dir/www.ini" > /dev/null; then
		sed -i 's/\(\[handlers\]\)/upstream_app_role =\n\n\1/' "$pub_cnf_dir/www.ini"
	fi
	if ! grep 'change_master_timeout' "$pub_cnf_dir/mysql.ini" > /dev/null; then
		sed -i 's/\(\[handlers\]\)/change_master_timeout = 30\n\n\1/' "$pub_cnf_dir/mysql.ini"
	fi

fi

if compare_versions $installed_version lt '0.7.2-2'; then
	if [ -f /etc/mysql/farm-replication.cnf ]; then
		server_id=$(grep 'server-id' /etc/mysql/farm-replication.cnf)
		sed -i "s/\(\[mysqld\]\)/\1\n$server_id/1" /etc/my.cnf
		sed -i 's/.*farm-replication.cnf//' /etc/my.cnf
		rm -f /etc/mysql/farm-replication.cnf
	fi
fi

if compare_versions $installed_version lt '0.7.10-2'; then
	if [ -f $priv_cnf_dir/mysql.ini ] &&  [ "1" = "`grep 'replication_master' $priv_cnf_dir/mysql.ini | awk '{print $3}'`" ]; then
		if ! grep 'server-id' /etc/my.cnf > /dev/null; then
			sed -i "s/\(\[mysqld\]\)/\1\nserver-id = 1/1" /etc/my.cnf
		fi
	fi  
fi

if compare_versions $installed_version lt '0.7.14-1'; then
	if [ -f /mnt/privated.img ]; then
		umount $priv_cnf_dir 2>&1 || :
		mpoint=$(mktemp -d)
		mount /mnt/privated.img $mpoint -o loop
		rsync -a $mpoint/ /etc/scalr/private.d/
		sync 
		umount $mpoint
		rm -rf $mpoint
		rm -f /mnt/privated.img
	fi
fi


if compare_versions $installed_version lt '0.7.23-1'; then
	if grep 'logs_dir_prefix' "$pub_cnf_dir/script_executor.ini" > /dev/null; then
		sed -i 's/logs_dir_prefix.*//g' "$pub_cnf_dir/script_executor.ini"
	fi
	if ! grep 'logs_dir' "$pub_cnf_dir/script_executor.ini" > /dev/null; then
			cat <<EOF >> "$pub_cnf_dir/script_executor.ini"
			
logs_dir=/var/log/scalarizr/scripting
EOF
	fi
fi

if compare_versions $installed_version lt '0.7.29-1'; then
	if ! grep 'scalarizr.handlers.deploy' "$pub_cnf_dir/config.ini" > /dev/null; then
		cat <<EOF >> "$pub_cnf_dir/config.ini"
			
; Deployments
; @optional
deploy = scalarizr.handlers.deploy
EOF
	fi
fi

if compare_versions $installed_version lt '0.7.45-1'; then
	dbfile="$priv_cnf_dir/db.sqlite"
	if [ -e "$dbfile" ]; then
			%{__python} <<EOF
import sqlite3
import os
conn = sqlite3.Connection('${dbfile}')
cur = conn.cursor()
cur1 = conn.cursor()
for row in cur.execute('select device from storage where state = ?', ('attached', )):
	device = row[0]
	if not os.path.exists(device):
		cur1.execute('delete from storage where device = ? and state = ?', (device, 'attached'))
conn.commit()
EOF
	fi
fi

if compare_versions "$installed_version" lt '0.7.78-1'; then
	rm -rf "$priv_cnf_dir/hosts"
fi


if compare_versions "$installed_version" lt '0.7.93-1'; then
	val=$(curl http://169.254.169.254/latest/user-data/ 2>&1 | \
		%{__python} -c "import sys, re; print re.search(r'cloud_storage_path=([^;]+)', sys.stdin.read()).group(1).strip().replace('/', '\\/');")
	if ! grep 'cloud_storage_path' "$priv_cnf_dir/config.ini" > /dev/null; then
		sed -i "s/\(\[general\]\)/\1\ncloud_storage_path=$val/1" "$priv_cnf_dir/config.ini"
	fi
fi

if compare_versions "$installed_version" lt '0.7.97-1'; then
	if ! grep 'mysqldump_options' "$pub_cnf_dir/mysql.ini" > /dev/null; then
		sed -i 's/\(\[handlers\]\)/mysqldump_options = --create-options --routines --add-drop-database --quick --quote-names --flush-privileges\n\n\1/' "$pub_cnf_dir/mysql.ini"
	fi
fi


if compare_versions "$installed_version" lt '0.7.149-1'; then
	# Add new colums
	dbfile="$priv_cnf_dir/db.sqlite"
	if [ -e "$dbfile" ]; then
		%{__python} -c "
import sqlite3
import os
conn = sqlite3.Connection('${dbfile}')
cur = conn.cursor()
if not cur.execute('pragma table_info(state)').fetchall():
	cur.execute('CREATE TABLE state ("name" PRIMARY KEY ON CONFLICT REPLACE, "value" TEXT)')
	conn.commit()
cur.close()
"
	fi
fi


if compare_versions "$installed_version" lt '0.7.192-1'; then
	cat <<EOF >> /etc/yum.repos.d/scalr.repo

[scalr-delayed]
name=scalr-delayed
baseurl=http://rpm-delayed.scalr.net/rpm/rhel/\$releasever/\$basearch
enabled=1
gpgcheck=0
EOF
fi


if compare_versions "$installed_version" lt '0.7.199-1'; then
	if [ -e "$priv_cnf_dir/config.ini" ]; then
		%{__python} -c "
import ConfigParser

name = '$priv_cnf_dir/config.ini'

conf = ConfigParser.ConfigParser()
conf.read(name)
conf.set('general', 'env_id', '')
conf.set('general', 'role_id', '')
conf.set('general', 'farm_role_id', '')

fp = open(name, 'w+')
conf.write(fp)
fp.close()
"
	fi
fi


if compare_versions "$installed_version" lt '0.7.212-1'; then
	if grep 'app' "$pub_cnf_dir/config.ini" > /dev/null; then
		sed -i 's/Include\sprivate.d\/vhosts\/\*\.vhost.conf//1' '/etc/httpd/conf/httpd.conf'
		sed -i 's/Include\s\/etc\/scalr\/private.d\/private.d\/vhosts\/\*\.vhost.conf//1' '/etc/httpd/conf/httpd.conf'
	fi
fi


if compare_versions "$installed_version" lt '0.7.228-1'; then
	%{__python} - <<-EOF
		import os
		verfile = '/etc/scalr/private.d/.scalr-version'
		if os.path.exists(verfile):
		    ver = open(verfile).read().replace('\n', '').strip()
		    open(verfile, 'w').write(ver)
	EOF
fi

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
		${__python} - <<-EOF
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

sync
umount -l "$priv_cnf_dir" 2>&1 || :	


%posttrans
set -x

pub_cnf_dir='/etc/scalr/public.d'
priv_cnf_dir='/etc/scalr/private.d'

/sbin/chkconfig --add scalarizr
/sbin/chkconfig --add scalarizr_update
chmod +x /etc/init.d/scalarizr
chmod +x /etc/init.d/scalarizr_update

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

/sbin/service scalarizr condrestart > /dev/null 2>&1 || :

%preun
set -x

pub_cnf_dir='/etc/scalr/public.d'
priv_cnf_dir='/etc/scalr/private.d'
szr_version_file='/tmp/.szr-version'

if [ $1 = 0 ]; then
	/sbin/service scalarizr stop > /dev/null 2>&1 || :
	/sbin/chkconfig --del scalarizr
	/sbin/chkconfig --del scalarizr_update
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
cp "%{_sourcedir}/scalarizr_update.init" "%{buildroot}%{_initrddir}/scalarizr_update"


%clean
rm -rf "$RPM_BUILD_ROOT"


%files
%defattr(-,root,root)
/usr
%config	           %{_initrddir}/scalarizr
%config            %{_initrddir}/scalarizr_update
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

