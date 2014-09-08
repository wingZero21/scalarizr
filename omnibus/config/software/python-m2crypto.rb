name "python-m2crypto"
pypi_name = "M2Crypto"
default_version "0.21.1"

dependency "python"
dependency "cacerts"
dependency "python-openssl"
dependency "gdbm"
dependency "ncurses"


if windows?
  source :url => "https://github.com/saltstack/salt-windows-install/blob/master/deps/win32-py2.7/M2Crypto-0.21.1.win32-py2.7.msi?raw=true",
         :md5 => "3e2a1207b4b55037e21592278fd166ea"
  build do
    "#{install_dir}/embedded/python/Scripts/easy_install.exe C:/omnibus-ruby/src/M2Crypto-0.21.1.win32-py2.7.msi"
  end
elsif ohai['platform_family'] == "debian"
  build do
    command "export CFLAGS=-I#{install_dir}/embedded/include/python2.7 && #{install_dir}/embedded/bin/pip install -I #{pypi_name}==#{default_version}"
  end
else
  source :url => "https://pypi.python.org/packages/source/M/M2Crypto/M2Crypto-#{default_version}.tar.gz",
         :md5 => 'f93d8462ff7646397a9f77a2fe602d17'

  relative_path "M2Crypto-#{default_version}"

  build do
    command "sed -i 's|python|#{install_dir}/embedded/bin/python|g' /var/cache/omnibus/src/M2Crypto-#{default_version}/fedora_setup.sh"
    command "echo '[easy_install]' >> /var/cache/omnibus/src/M2Crypto-#{default_version}/setup.cfg"
    command "echo 'zip_ok = 0' >> /var/cache/omnibus/src/M2Crypto-#{default_version}/setup.cfg"
    command "export CFLAGS=-I#{install_dir}/embedded/include/python2.7 && /var/cache/omnibus/src/M2Crypto-#{default_version}/fedora_setup.sh install --prefix=#{install_dir}/embedded"
  end  
end
