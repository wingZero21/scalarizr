name "python-openssl"
pypi_name = "pyOpenSSL"
default_version "0.13.1"

dependency "python"
source :url => "http://slproweb.com/download/Win32OpenSSL_Light-1_0_1i.exe",
       :md5 => "c183d1946ffad45105bdd9ed047f7d9d"

package_src = "C:/omnibus-ruby/src/gdbm/gdbm-#{default_version}.exe"


build do
  command "call #{windows_safe_path(package_src)} /sp /verysilent /suppressmsgboxes"
  command "#{install_dir}/embedded/bin/pip install #{pypi_name}==#{default_version}"
end
