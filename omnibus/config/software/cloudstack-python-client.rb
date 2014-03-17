name "cloudstack-python-client"
version "0.2.3"

dependency "python"

source = "git+git://github.com/Scalr/python-cloudstack.git@#{version}"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{source}"
end
