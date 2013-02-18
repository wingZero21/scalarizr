#!/bin/sh

pk=$1
host=$2

echo "Create tarball"
cd ../src
rm -f scalarizr.tar.gz
tar -czf scalarizr.tar.gz scalarizr
echo "Uploading"
scp -i $pk -o stricthostkeychecking=no scalarizr.tar.gz root@$host:/root/
echo "Extracting"
ssh -i $pk -o stricthostkeychecking=no -l root $host tar -xzf /root/scalarizr.tar.gz -C /usr/lib/python2.6/site-packages/
echo "Done"
