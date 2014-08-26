name "pip"
default_version "1.5.6"

dependency "python"
dependency "setuptools"

source :url => "http://pypi.python.org/packages/source/p/pip/pip-#{version}.tar.gz",
       :md5 => '01026f87978932060cc86c1dc527903e'

relative_path "pip-#{version}"

if windows?
  build do
    command "#{install_dir}/embedded/python/python.exe setup.py install"
  end
else
  build do
    command "#{install_dir}/embedded/bin/python setup.py install --prefix=#{install_dir}/embedded"
  end
end
