require 'ohai'

name "python-m2crypto"
pypi_name = "M2Crypto"
version "0.21.1"

dependency "python"

if OHAI.platform_family == "debian"
  build do
    command "export CFLAGS=-I#{install_dir}/embedded/include/python2.7 && #{install_dir}/embedded/bin/pip install -I --build #{project_dir} #{pypi_name}==#{version}"
  end
end

if OHAI.platform_family == "rhel"
  source :url => "https://pypi.python.org/packages/source/M/M2Crypto/M2Crypto-#{version}.tar.gz",
         :md5 => 'f93d8462ff7646397a9f77a2fe602d17'

  relative_path "M2Crypto-#{version}"

  build do
    command "sed -i 's|python|#{install_dir}/embedded/bin/python|g' /var/cache/omnibus/src/M2Crypto-#{version}/fedora_setup.sh"
    command "echo '[easy_install]' >> /var/cache/omnibus/src/M2Crypto-#{version}/setup.cfg"
    command "echo 'zip_ok = 0' >> /var/cache/omnibus/src/M2Crypto-#{version}/setup.cfg"
    command "export CFLAGS=-I#{install_dir}/embedded/include/python2.7 && /var/cache/omnibus/src/M2Crypto-#{version}/fedora_setup.sh install --prefix=#{install_dir}/embedded"
  end
end
