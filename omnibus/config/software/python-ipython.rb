name "python-ipython"
pypi_name = "ipython"
default_version "2.3.0"

dependency "pip"
dependency "python-pygments" # syntax higlighting

if windows?
    dependency "python-pyreadline" # win-specific autocompletion
    pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
else
    dependency "python-readline"
    pip = "#{install_dir}/embedded/bin/pip"
end

build do
    command "#{pip} install -I #{pypi_name}==#{default_version}"
end
