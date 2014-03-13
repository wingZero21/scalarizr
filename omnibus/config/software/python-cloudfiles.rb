name "python-cloudfiles"
pypi_name = "python-cloudfiles"
version "1.7.10"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
