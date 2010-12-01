#
# Cookbook Name:: apache2
# Recipe:: python 
#
# Copyright 2008-2009, Opscode, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

case node[:platform]
when "debian","ubuntu"
  package "libapache2-mod-rpaf"
when "redhat","centos"
  package "httpd-devel" do
        action :install
  end
  execute "rpmbuild --rebuild http://yum.cocoiti.com/CentOS/5/RPMS/SRPMS/mod_rpaf-0.6-2.src.rpm"
  execute "mod_rpaf" do
        if node[:kernel][:machine] == "x86_64"
                command "rpm -ivh /usr/src/redhat/RPMS/x86_64/mod_rpaf-0.6-2.x86_64.rpm"
        else
                command "rpm -ivh /usr/src/redhat/RPMS/i386/mod_rpaf-0.6-2.i386.rpm"
        end
  end
  execute "yum -y erase httpd-devel apr-devel apr-util-devel cyrus-sasl-devel db4-devel expat-devel openldap-devel"
  cookbook_file "/etc/httpd/conf.d/mod_rpaf.conf" do
    source "mod_rpaf.conf"
    mode 0755
    owner "root"
    group "root"
  end
end
