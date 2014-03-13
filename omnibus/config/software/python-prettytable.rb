name "python-prettytable"
pypi_name = "PrettyTable"
version "0.7.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
