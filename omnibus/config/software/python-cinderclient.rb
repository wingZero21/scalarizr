name "python-cinderclient"
pypi_name = "python-cinderclient"
version "1.0.5"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
