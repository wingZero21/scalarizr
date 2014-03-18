name "python-pychef"
pypi_name = "PyChef"
default_version "0.2.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
