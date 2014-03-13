name "python-keystoneclient"
pypi_name = "python-keystoneclient"
version "0.3.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
