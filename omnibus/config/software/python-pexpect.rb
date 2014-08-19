name "python-pexpect"
pypi_name = "pexpect"
default_version "2.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
