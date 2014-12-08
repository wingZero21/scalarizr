name "pyreadline"
pypi_name = "pyreadline"
default_version "2.0"

dependency "pip"

build do
    command "#{pip} install -I #{pypi_name}==#{default_version}"
end
