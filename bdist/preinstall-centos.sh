#!/bin/bash

yum -y update


# Install python2.6
echo "[ius]" > /etc/yum.repos.d/IUS.repo
echo "name=IUS" >> /etc/yum.repos.d/IUS.repo
echo "baseurl=http://dl.iuscommunity.org/pub/ius/stable/Redhat/5/i386/" >> /etc/yum.repos.d/IUS.repo
echo "enabled=1" >> /etc/yum.repos.d/IUS.repo
echo "gpgcheck=0" >> /etc/yum.repos.d/IUS.repo

yum -y install python26 python26-setuptools


# Install scalarizr dependencies
# Boto
easy_install-2.6 boto

# M2Crypto
yum -y install swig openssl-devel gcc
cd /root
wget http://pypi.python.org/packages/source/M/M2Crypto/M2Crypto-0.20.2.tar.gz
tar -xzf M2Crypto-0.20.2.tar.gz
cd M2Crypto-0.20.2
python2.6 setup.py build build_ext -I/usr/include/openssl
python2.6 setup.py test
python2.6 setup.py install


mkdir -p /opt/scalarizr
cd /opt/scalarizr
