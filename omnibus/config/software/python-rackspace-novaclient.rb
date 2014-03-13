name "python-rackspace-novaclient"
pypi_name = "rackspace-novaclient"
version "1.3"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
