name "cloudstack-python-client"
default_version "0.2.3"

dependency "python"

source = "git+git://github.com/Scalr/python-cloudstack.git@#{default_version}"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{source}"
end
