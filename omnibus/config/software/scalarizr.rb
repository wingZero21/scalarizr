name "scalarizr"

dependency "scalarizr-deps"

source :path => ENV['BUILD_DIR']
if windows?
    install_dir = "C:/opt/scalarizr"
    python = "#{install_dir}/embedded/python/python.exe"
    prefix = "#{install_dir}/embedded/python"
else
    python = "#{install_dir}/embedded/bin/python"
    prefix = "--prefix=#{install_dir}/embedded"
end
build do
  command "#{python} setup_omnibus.py install --prefix=#{prefix} --install-scripts #{install_dir}/bin"
  command "sed -i 's/\\#\\!\\/usr\\/bin\\/python/\\#\\!\\/opt\\/scalarizr\\/embedded\\/bin\\/python/1' #{install_dir}/scripts/*"
end
