name "python-novaclient"
pypi_name = "python-novaclient"
default_version "2.15.0"

dependency "pip"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
