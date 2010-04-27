#!/bin/sh
nosetests --cover-erase --with-coverage 2>&1 | grep scalarizr
