name "python-pymongo"
pypi_name = "pymongo"
default_version "2.6.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
