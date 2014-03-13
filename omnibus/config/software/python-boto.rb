name "python-boto"
pypi_name = "boto"
version "2.13.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
