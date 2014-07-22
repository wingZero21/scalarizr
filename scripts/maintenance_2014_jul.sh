#!/bin/bash

set -x

function get_ini() {
	grep "^$2" /etc/scalr/$1 | awk '{print $3}'
}

function get_repo() {
	local farm_role_id=$(get_ini 'private.d/config.ini' 'farm_role_id')
	if [ -n $farm_role_id ]; then
		szradm_exec=$(which szradm || echo -n '/usr/local/bin/szradm')
		local xml=$($szradm_exec -q list-farm-role-params farm-role-id=$farm_role_id)
		local python=$([[ $(python -V 2>&1) == *2.4.* ]] && echo -n 'python26' || echo -n 'python')
		read -d '' pyxpath <<-EOC
			import sys;
			import xml.etree.ElementTree as ET;
			xml = ET.fromstring(sys.stdin.read());
			print xml.find(sys.argv[1]).text.strip();
			EOC
		echo $xml | $python -c "$pyxpath" '*/update/repository'
	else
		get_repo_bad_way	
	fi
}

function get_repo_bad_way() {
	grep -q latest /etc/scalr/updclient.ini && echo -n 'latest' || echo -n 'stable'
}

repo=$(get_repo)
is_latest=$([ $repo = 'latest' ] && echo -n 'true' || echo -n 'false')
platform=$(get_ini 'public.d/config.ini' 'platform')

if which apt-get 1>&2 2>/dev/null; then
	rm -f /etc/apt/sources.list.d/scalr*
	url=$($is_latest && echo -n "http://apt.scalr.net/debian scalr/" || echo -n "http://apt-delayed.scalr.net/debian scalr/")
	echo "deb $url" > "/etc/apt/sources.list.d/scalr-$repo.list"
	apt-get update
	if ! $is_latest; then
		apt-get install -y --force-yes scalr-upd-client
	fi
	apt-get install -y --force-yes scalarizr-$platform
else
	rm -f /etc/yum.repos.d/scalr*
	url=$($is_latest && echo -n 'http://rpm.scalr.net/rpm/rhel/$releasever/$basearch' || echo -n 'http://rpm-delayed.scalr.net/rpm/rhel/$releasever/$basearch')
	cat <<-EOC > /etc/yum.repos.d/scalr-$repo.repo
	[$repo]
	name=$repo
	baseurl=$url
	enabled=1
	gpgcheck=0
	EOC
	yum clean all
	if ! $is_latest; then
		yum update -y scalr-upd-client
	fi
	yum update -y scalarizr-$platform
fi 
service scalarizr start
sleep 5
service scalr-upd-client restart