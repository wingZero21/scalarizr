name "python-simplejson"
pypi_name = "simplejson"
version "3.3.0"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
