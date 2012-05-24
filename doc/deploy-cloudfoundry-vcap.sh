#!/bin/bash

mkdir -p cloudfoundry && cd cloudfoundry

apt-get update
apt-get -y -q install git-core

git clone https://github.com/cloudfoundry/vcap.git
cd vcap
git checkout 8f37ade2bdb799b561a4409eef518667fce69b00


curl -L get.rvm.io | bash -s stable


