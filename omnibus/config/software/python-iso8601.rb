name "python-iso8601"
pypi_name = "iso8601"
default_version "0.1.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
