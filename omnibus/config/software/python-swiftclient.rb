name "python-swiftclient"
pypi_name = "python-swiftclient"
version "1.7.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
