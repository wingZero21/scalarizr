#!/bin/bash

apt-get -y update

apt-get -y install python-setuptools python-dev swig libssl-dev gcc

easy_install boto m2crypto

mkdir /opt/scalarizr


