name "python-openssl"
pypi_name = "pyOpenSSL"
default_version "0.13.1"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
