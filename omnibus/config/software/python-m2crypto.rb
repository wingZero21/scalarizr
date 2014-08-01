require 'ohai'

name "python-m2crypto"
pypi_name = "M2Crypto"
default_version "0.22.3"

dependency "pip"

build do
    command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end

