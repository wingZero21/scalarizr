#!/bin/sh

pk=$1
host=$2

echo "Create tarball"
cd ../src
rm -f scalarizr.tar.gz
tar -czf scalarizr.tar.gz scalarizr
echo "Uploading"
scp -i $pk scalarizr.tar.gz root@$host:/mnt
echo "Extracting"
ssh -i $pk -l root $host tar -xzf /mnt/scalarizr.tar.gz -C /var/lib/python-support/python2.5
ssh -i $pk -l root $host tar -xzf /mnt/scalarizr.tar.gz -C /var/lib/python-support/python2.6
echo "Done"
