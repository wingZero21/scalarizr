name "google-api-python-client"
pypi_name = "google_api_python_client"
version "1.2"

dependency "python"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
end
