name "python-pygments"
pypi_name = "pygments"
default_version "2.0.1"

dependency "pip"

if windows?
    pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
    pip = "#{install_dir}/embedded/bin/pip"
end

build do
    command "#{pip} install -I #{pypi_name}==#{default_version}"
end
