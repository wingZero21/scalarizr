name "python-pexpect"
pypi_name = "pexpect"
default_version "3.3"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
