name "python-requests"
pypi_name = "requests"
default_version "1.2.3"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
