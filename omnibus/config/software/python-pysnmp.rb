name "python-pysnmp"
pypi_name = "pysnmp"
default_version "4.2.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
