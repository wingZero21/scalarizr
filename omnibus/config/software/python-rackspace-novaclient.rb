name "python-rackspace-novaclient"
pypi_name = "rackspace-novaclient"
default_version "1.3"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
