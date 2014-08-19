name "python-pymysql"
pypi_name = "PyMySQL"
default_version "0.5"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
