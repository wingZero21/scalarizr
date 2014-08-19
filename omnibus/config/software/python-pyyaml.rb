name "python-pyyaml"
default_version "3.11"

dependency "python"
dependency "libyaml"

source :url => "https://pypi.python.org/packages/source/P/PyYAML/PyYAML-#{default_version}.tar.gz",
       :md5 => 'f50e08ef0fe55178479d3a618efe21db'

relative_path "PyYAML-#{default_version}"

build do
  command "#{install_dir}/embedded/bin/python setup.py --with-libyaml install --prefix=#{install_dir}/embedded"
end
