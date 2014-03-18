name "python-simplejson"
pypi_name = "simplejson"
default_version "3.3.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
