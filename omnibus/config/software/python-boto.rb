name "python-boto"
pypi_name = "boto"
default_version "2.13.0"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
