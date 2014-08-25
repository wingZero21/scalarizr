name "python-pysnmp-mibs"
pypi_name = "pysnmp-mibs"
default_version "0.1.4"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
