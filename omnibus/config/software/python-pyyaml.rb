name "python-pyyaml"
default_version "3.11"

dependency "python"
dependency "libyaml"

source :url => "https://pypi.python.org/packages/source/P/PyYAML/PyYAML-#{default_version}.tar.gz",
       :md5 => '74c94a383886519e9e7b3dd1ee540247'

relative_path "PyYAML-#{default_version}"

build do
  command "#{install_dir}/embedded/bin/python setup.py --with-libyaml install --prefix=#{install_dir}/embedded"
end
