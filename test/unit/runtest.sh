
if readlink -f "$0" > /dev/null 2>&1
then
  runner=`readlink -f "$0"`
else
  runner="$0"
fi

testdir=`dirname "$runner"`
basedir=`dirname "$testdir"`

pypath=$basedir/../src:$testdir/testcases

export PYTHONPATH=$pypath

python $*