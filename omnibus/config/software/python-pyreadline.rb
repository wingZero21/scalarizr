name "python-pyreadline"
pypi_name = "pyreadline"
default_version "2.0"

dependency "pip"

build do
    pip = "#{install_dir}/embedded/bin/pip"
    command "#{pip} install -I #{pypi_name}==#{default_version}"
end
