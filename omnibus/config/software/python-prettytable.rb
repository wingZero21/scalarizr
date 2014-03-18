name "python-prettytable"
pypi_name = "PrettyTable"
default_version "0.7.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
