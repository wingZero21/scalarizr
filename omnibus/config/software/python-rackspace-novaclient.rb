name "python-rackspace-novaclient"
pypi_name = "rackspace-novaclient"
default_version "1.3"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
