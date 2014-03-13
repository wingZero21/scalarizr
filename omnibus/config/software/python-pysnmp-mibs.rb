name "python-pysnmp-mibs"
pypi_name = "pysnmp-mibs"
version "0.1.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
