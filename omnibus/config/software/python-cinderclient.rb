name "python-cinderclient"
pypi_name = "python-cinderclient"
default_version "1.0.5"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
