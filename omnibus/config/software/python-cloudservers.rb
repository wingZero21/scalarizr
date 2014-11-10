name "python-cloudservers"
pypi_name = "python-cloudservers"
default_version "1.2"

dependency "pip"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
