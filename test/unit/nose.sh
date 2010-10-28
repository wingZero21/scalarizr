#!/bin/sh
self=$(dirname $(readlink -f $0))

path[0]="$self/testcases"
path[1]=$(readlink -f "$self/../../src")
export PYTHONPATH="${path[0]}:${path[1]}"

#nosetests -v -w "$self/testcases/szr_unittest" --with-coverage --cover-package scalarizr --cover-erase $@
nosetests -v -w "$self/testcases/szr_unittest" $@


