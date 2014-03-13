name "python-novaclient"
pypi_name = "python-novaclient"
version "2.15.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
