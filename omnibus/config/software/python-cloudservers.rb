name "python-cloudservers"
pypi_name = "python-cloudservers"
default_version "1.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
