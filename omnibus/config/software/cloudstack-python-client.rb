name "cloudstack-python-client"
default_version "0.2.4"

dependency "pip"

source = "git+git://github.com/Scalr/python-cloudstack.git@#{default_version}"

if windows?
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
  pip = "#{install_dir}/embedded/bin/pip"
end

build do
  command "#{pip} install -I #{source}"
end
