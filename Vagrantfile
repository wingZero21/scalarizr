# -*- mode: ruby -*-
# vi: set ft=ruby :

packages = [
  "python-nose",
  "python-mock",
  "python-wsgi-intercept",
  "python-m2crypto",
  "python-swiftclient",
  "python-cinderclient",
  "python-novaclient",
  "python-novaclient-os-diskconfig-ext",
  "python-novaclient-rax-backup-shedule-ext"
#  "python-rackspace-novaclient"
].join(" ")

Vagrant::Config.run do |config|
  config.vm.define :ubuntu do |ubuntu|
    ubuntu.vm.box = "ubuntu1204"
    ubuntu.vm.provision :shell, :inline => <<-EOF
      grep vagrant /root/.bashrc || echo 'export PYTHONPATH=/vagrant/src' >> /root/.bashrc
      export DEBIAN_FRONTEND=noninteractive
      export DEBIAN_PRIORITY=critical 
      if ! test -f  /etc/apt/sources.list.d/scalr.list; then
        wget http://apt.scalr.net/scalr-repository_0.3_all.deb
        dpkg -i scalr-repository_0.3_all.deb
        rm -f scalr-repository_0.3_all.deb
        rm -f /etc/apt/sources.list.d/scalr-*
        echo 'deb http://buildbot.scalr-labs.com/apt/debian scalr/' > /etc/apt/sources.list.d/scalr.list
      fi
      apt-get update
      apt-get install -y --fix-missing #{packages}
      EOF
  end

  config.vm.define :centos do |centos|
    centos.vm.box = "centos63"
    centos.vm.provision :shell, :inline => <<-EOF
      grep vagrant /root/.bashrc || echo 'export PYTHONPATH=/vagrant/src' >> /root/.bashrc
      if ! test -f  /etc/yum.repos.d/scalr.repo; then
        cat <<EOC > /etc/yum.repos.d/scalr.repo
[scalr]
name=scalr
baseurl=http://buildbot.scalr-labs.com/rpm/trunk/rhel/\$releasever/\$basearch
enabled=1
gpgcheck=0
EOC
      fi
      yum install -y #{packages}
    EOF
  end
end
