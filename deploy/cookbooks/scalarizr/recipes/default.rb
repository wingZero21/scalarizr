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
	execute "echo 'deb http://local.webta.net/apt/dev scalr/' > /etc/apt/sources.list.d/scalr.list"
	execute "export DEBIAN_FRONTEND=noninteractive"
	execute "apt-get update && apt-get -y install scalarizr-" + node[:scalarizr][:platform]
when "redhat","centos"
    cookbook_file "/etc/yum.repos.d/scalr.repo" do
		source "scalr-rh.repo"
	end		
	execute "yum -y install scalarizr-" + node[:scalarizr][:platform]
when "fedora"
	cookbook_file "/etc/yum.repos.d/scalr.repo" do
        source "scalr-fedora.repo"
    end
    execute "yum -y install scalarizr-" + node[:scalarizr][:platform]
end

behaviours=node[:scalarizr][:behaviour].join(",")
execute "scalarizr -y --configure -o behaviour=#{behaviour} -o platform=" + node[:scalarizr][:platform] 

