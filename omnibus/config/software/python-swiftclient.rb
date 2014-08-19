name "python-swiftclient"
pypi_name = "python-swiftclient"
default_version "2.2.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
