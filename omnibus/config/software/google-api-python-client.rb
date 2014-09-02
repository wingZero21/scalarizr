name "google-api-python-client"
pypi_name = "google-api-python-client"
default_version "1.2"

dependency "pip"
dependency "python-pyyaml"
if windows?
    source :url => "https://google-api-python-client.googlecode.com/files/google-api-python-client-#{default_version}.tar.gz",
           :md5 => "031c69eacdd25606782d045b17f54934"
    relative_path "google-api-python-client-#{default_version}"
    build do

        command "#{install_dir}/embedded/python/python.exe setup.py build"
        command "#{install_dir}/embedded/python/python.exe setup.py install"
    end
else
    pip = "#{install_dir}/embedded/bin/pip"
    build do
        command "#{pip} install -I #{pypi_name}==#{default_version}"
    end
end

