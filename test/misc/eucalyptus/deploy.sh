#!/bin/sh

scp -i $1 rebundle.xml $2:/mnt/
scp -i $1 ../../../bin/msgsnd $2:/mnt/
scp -i $1 /etc/euca2ools/euca2-Scalr-x509/euca2-Scalr-3c1d8b1a-cert.pem $2:/mnt/euca2-cert.pem
scp -i $1 /etc/euca2ools/euca2-Scalr-x509/euca2-Scalr-3c1d8b1a-pk.pem $2:/mnt/euca2-pk.pem
scp -i $1 /etc/euca2ools/euca2-Scalr-x509/cloud-cert.pem $2:/mnt/

ssh -i $1 -t $2 <<CMD
echo "[euca2ools]" > /etc/yum.repos.d/euca2ools.repo
echo "name=Euca2ools" >> /etc/yum.repos.d/euca2ools.repo
echo "baseurl=http://www.eucalyptussoftware.com/downloads/repo/euca2ools/1.3.1/yum/centos/" >> /etc/yum.repos.d/euca2ools.repo
echo "enabled=1" >> /etc/yum.repos.d/euca2ools.repo
echo "gpgcheck=0" >> /etc/yum.repos.d/euca2ools.repo

echo "[scalarizr]" > /etc/yum.repos.d/scalr.repo
echo "name=scalarizr" >> /etc/yum.repos.d/scalr.repo
echo "baseurl=http://rpm.scalr.net/rpm/rhel/5/x86_64" >> /etc/yum.repos.d/scalr.repo
echo "enabled=1" >> /etc/yum.repos.d/scalr.repo
echo "gpgcheck=0" >> /etc/yum.repos.d/scalr.repo

rpm -Uvh http://download.fedora.redhat.com/pub/epel/5/i386/epel-release-5-4.noarch.rpm

yum -y install euca2ools.x86_64 scalarizr rsync

sed -i 's/#!\/usr\/bin\/python/#!\/usr\/bin\/python2.6/g' /mnt/msgsnd

CMD

