#!/bin/sh

pk=$1
host=$2

echo "Create tarball"
cd ../src
rm -f scalarizr.tar.gz
tar -czf scalarizr.tar.gz scalarizr
echo "Uploading"
scp -i $pk scalarizr.tar.gz ubuntu@$host:/home/ubuntu
echo "Extracting"
ssh -i $pk -l ubuntu $host sudo tar -xzf /home/ubuntu/scalarizr.tar.gz -C /var/lib/python-support/python2.5
ssh -i $pk -l ubuntu $host sudo tar -xzf /home/ubuntu/scalarizr.tar.gz -C /var/lib/python-support/python2.6
echo "Done"