# this is a win-specific package
name "python-pyreadline"
pypi_name = "pyreadline"
default_version "2.0"

dependency "pip"

pip = "#{install_dir}/embedded/python/Scripts/pip.exe"

build do
    command "#{pip} install -I #{pypi_name}==#{default_version}"
end
