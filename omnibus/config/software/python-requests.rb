name "python-requests"
pypi_name = "requests"
version "1.2.3"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
