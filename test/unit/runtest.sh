
if readlink -f "$0" > /dev/null 2>&1
then
  runner=`readlink -f "$0"`
else
  runner="$0"
fi

base_dir=`dirname "$runner"`
src_dir=$(readlink -f "$base_dir/../../src")

pypath="$src_dir:$base_dir/testcases"
echo $pypath

export PYTHONPATH=$pypath

python $*
