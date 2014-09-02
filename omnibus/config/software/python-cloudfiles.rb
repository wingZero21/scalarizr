name "python-cloudfiles"
pypi_name = "python-cloudfiles"
default_version "1.7.10"

dependency "pip"
dependency "python-pyyaml"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip} install #{pypi_name}==#{default_version}"
end
