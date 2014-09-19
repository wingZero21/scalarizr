name "python-cryptography"
pypi_name = "cryptography"
default_version "0.5.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
