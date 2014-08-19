name "python-cloudfiles"
pypi_name = "python-cloudfiles"
default_version "1.7.11"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
