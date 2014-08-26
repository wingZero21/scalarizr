name "python-keystoneclient"
pypi_name = "python-keystoneclient"
default_version "0.3.2"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
