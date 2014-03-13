name "cloudstack-python-client"
version "0.2.2"

dependency "python"

source = "git+git@github.com:Scalr/python-cloudstack.git#egg=cloudstack"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} -e #{source}"
end
