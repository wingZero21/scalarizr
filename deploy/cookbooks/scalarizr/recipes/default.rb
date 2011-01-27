#
# Cookbook Name:: scalarizr
# Recipe:: default
#
# Copyright 2010, Scalr Inc.
#
# All rights reserved - Do Not Redistribute
#
case node[:platform]
when "debian","ubuntu"
	execute "cd /tmp && wget http://apt.scalr.net/scalr-repository_0.2_all.deb && dpkg -i /tmp/scalr-repository_0.2_all.deb && rm -f /tmp/scalr-repository_0.2_all.deb"
	if node[:scalarizr][:dev] == "1"
		execute "echo 'deb http://local.webta.net/apt/dev scalr/' > /etc/apt/sources.list.d/scalr.list"
	end
	execute "apt-get update && apt-get -y install scalarizr-" + node[:scalarizr][:platform]
when "redhat","centos"
	cookbook_file "/etc/yum.repos.d/scalr.repo" do
		if node[:scalarizr][:dev] == "0"
			source "scalr-rh.repo"
		else
			source "scalr-rh-dev.repo"
		end
	end		
	execute "yum -y install scalarizr-" + node[:scalarizr][:platform]
when "fedora"
    cookbook_file "/etc/yum.repos.d/scalr.repo" do
        source "scalr-fedora.repo"
    end
    execute "yum -y install scalarizr-" + node[:scalarizr][:platform]
end

if node[:scalarizr][:dev] == "1"
	execute "cp /etc/scalr/logging-debug.ini /etc/scalr/logging.ini"
end

behaviours=node[:scalarizr][:behaviour].join(",")
execute "scalarizr -y --configure -o behaviour=" + behaviours + " -o platform=" + node[:scalarizr][:platform] 

