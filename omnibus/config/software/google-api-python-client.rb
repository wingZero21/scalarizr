name "google-api-python-client"
pypi_name = "google_api_python_client"
default_version "1.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
end
