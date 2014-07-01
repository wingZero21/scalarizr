#!/bin/bash

is_latest=$(grep -q latest /etc/scalr/updclient.ini && echo -n 'true' || echo -n 'false')
repo=$($is_latest && echo -n 'latest' || echo -n 'stable')
platform=$(grep '^platform' /etc/scalr/public.d/config.ini | awk '{print $3}')

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