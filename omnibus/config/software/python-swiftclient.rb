name "python-swiftclient"
pypi_name = "python-swiftclient"
default_version "1.7.0"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"

build do
  command "#{pip} -I #{pypi_name}==#{default_version}"
end
