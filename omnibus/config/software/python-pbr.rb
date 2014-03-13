name "python-pbr"
pypi_name = "pbr"
version "0.5.21"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
