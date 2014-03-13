name "python-pymongo"
pypi_name = "pymongo"
version "2.6.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
