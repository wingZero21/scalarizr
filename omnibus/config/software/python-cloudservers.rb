name "python-cloudservers"
pypi_name = "python-cloudservers"
version "1.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
