name "python-pychef"
pypi_name = "PyChef"
version "0.2.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
