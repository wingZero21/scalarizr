#!/bin/sh

ec2_ssh_key=$1
ec2_host=$2

# Upload to ec2 instance
# @param source
# @param dest
scp_ec2() {
	scp -i $ec2_ssh_key -o StrictHostKeyChecking=no $1 root@$ec2_host:$2
}

# Execute shell command on ec2 instance
# @param cmd
ssh_ec2() {
	ssh -i $ec2_ssh_key -o StrictHostKeyChecking=no -l root $ec2_host $1
}


ssh_ec2 <<EOF
wget http://dl.iuscommunity.org/pub/ius/stable/Redhat/5/i386/epel-release-1-1.ius.el5.noarch.rpm
wget http://dl.iuscommunity.org/pub/ius/stable/Redhat/5/i386/ius-release-1.0-6.ius.el5.noarch.rpm
rpm -Uvh epel-release-* ius-release-*
echo '[scalr]' > /etc/yum.repos.d/scalr.repo
echo 'name=scalr' >> /etc/yum.repos.d/scalr.repo
echo 'baseurl=http://rpm.scalr.net/rpm/rhel/\$releasever/\$basearch' >> /etc/yum.repos.d/scalr.repo
echo 'enabled=1' >> /etc/yum.repos.d/scalr.repo
echo 'gpgcheck=0' >> /etc/yum.repos.d/scalr.repo
yum -y install python26 scalarizr
EOF

