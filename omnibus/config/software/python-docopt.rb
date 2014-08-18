name "python-docopt"
pypi_name = "docopt"
default_version "0.6.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
