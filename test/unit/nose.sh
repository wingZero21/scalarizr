#!/bin/sh
find ./test/scalarizr -type d | grep -v .svn | xargs nosetests --with-coverage --cover-package=scalarizr --cover-erase

