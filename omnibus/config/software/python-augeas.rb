name "python-augeas"
default_version "5b4fe6568eca7866429180d82a7aed58ee824e37"

dependency "augeas"
dependency "pip"

source = "git+https://github.com/Scalr/python-augeas.git@#{default_version}"

build do
  command "#{install_dir}/embedded/bin/pip install -I #{source}"
end
