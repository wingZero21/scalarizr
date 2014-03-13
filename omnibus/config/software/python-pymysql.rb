name "python-pymysql"
pypi_name = "PyMySQL"
version "0.5"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
