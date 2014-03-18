name "python-novaclient"
pypi_name = "python-novaclient"
default_version "2.15.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
