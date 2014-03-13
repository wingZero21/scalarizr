name "python-pyyaml"
version "3.10"

dependency "python"
dependency "libyaml"

source :url => "https://pypi.python.org/packages/source/P/PyYAML/PyYAML-#{version}.tar.gz",
       :md5 => '74c94a383886519e9e7b3dd1ee540247'

relative_path "PyYAML-#{version}"

build do
  command "#{install_dir}/embedded/bin/python setup.py --with-libyaml install --prefix=#{install_dir}/embedded"
end
