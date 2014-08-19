name "python-boto"
pypi_name = "boto"
default_version "2.32.1"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
