name "python-iso8601"
pypi_name = "iso8601"
version "0.1.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
