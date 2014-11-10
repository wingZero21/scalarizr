name "python-cryptography"
pypi_name = "cryptography"
default_version "0.5"

dependency "python"
dependency "openssl"
dependency "libffi"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
