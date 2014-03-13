name "setuptools"
version "1.1.6"

dependency "python"

source :url => "https://pypi.python.org/packages/source/s/setuptools/setuptools-#{version}.tar.gz",
       :md5 => "ee82ea53def4480191061997409d2996"

relative_path "setuptools-#{version}"

build do
  command "#{install_dir}/embedded/bin/python setup.py install --prefix=#{install_dir}/embedded"
end
