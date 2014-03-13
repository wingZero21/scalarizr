name "python-pexpect"
pypi_name = "pexpect"
version "2.4"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
