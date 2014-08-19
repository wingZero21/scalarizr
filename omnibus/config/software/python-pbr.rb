name "python-pbr"
pypi_name = "pbr"
default_version "0.5.21"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
