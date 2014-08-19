name "python-keystoneclient"
pypi_name = "python-keystoneclient"
default_version "0.10.1"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
