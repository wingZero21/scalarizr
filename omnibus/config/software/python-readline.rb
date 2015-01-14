# this is a unix-specific package
name "python-readline"
pypi_name = "readline"
default_version "6.2.4.1"

dependency "pip"

pip = "#{install_dir}/embedded/bin/pip"

build do
    command "#{pip} install -I #{pypi_name}==#{default_version}"
end
