name "python-iso8601"
pypi_name = "iso8601"
default_version "0.1.4"

dependency "python"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip} install -I #{pypi_name}==#{default_version}"
end
