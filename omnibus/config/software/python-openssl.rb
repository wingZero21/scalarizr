name "python-openssl"
pypi_name = "pyOpenSSL"
version "0.13.1"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
