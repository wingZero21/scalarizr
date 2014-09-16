# TODO: check if this dependency can be completley skipped
# currently it's used here https://github.com/openstack/python-keystoneclient/blob/97c9ad19871f9831cda3bf8df0cd4d4c192cca45/keystoneclient/contrib/auth/v3/saml2.py#L13
name "lxml"

default_version "3.3.6"
if windows?
    source :url => "https://pypi.python.org/packages/2.7/l/lxml/lxml-#{default_version}.win-amd64-py2.7.exe",
           :md5 => '1b6b835a911bc6f399fcd176994cf683'
    if windows?
        easy_install = "#{install_dir}/embedded/python/Scripts/easy_install.exe"
        package_src = "C:/omnibus-ruby/src/lxml/lxml-#{default_version}.win-amd64-py2.7.exe"
        package_location = "#{install_dir}/embedded/python/Lib/site-packages/"
        build do  
          command "#{easy_install} --install-dir=#{package_location} #{package_src}"
        end
    end
else
    source :url => "https://pypi.python.org/packages/source/l/lxml/lxml-#{default_version}.tar.gz",
           :md5 => "a804b36864c483fe7abdd7f493a0c379"

    relative_path "lxml-#{version}"
    build do
        command "#{install_dir}/embedded/bin/python setup.py install --prefix=#{install_dir}/embedded"
    end
end