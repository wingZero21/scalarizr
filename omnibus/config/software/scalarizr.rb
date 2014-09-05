name "scalarizr"

dependency "scalarizr-deps"

if windows?
    source :path => "C:/Users/Administrator/AppData/Local/Temp/2/bootstrap/int-scalarizr"
    install_dir = "C:/opt/scalarizr"
    python = "#{install_dir}/embedded/python/python.exe"
    prefix = "#{install_dir}/embedded/python"
    scripts = "#{install_dir}/embedded/bin"
else
    source :path => ENV['BULD_DIR']
    python = "#{install_dir}/embedded/bin/python"
    prefix = "#{install_dir}/embedded"
    scripts = "#{install_dir}/bin"

end

if windows?
    build do
      command "#{windows_safe_path(python)} setup_omnibus.py install --prefix=#{windows_safe_path(prefix)} --install-scripts=#{windows_safe_path(scripts)}"
    end
else
    build do
      command "#{python} setup_omnibus.py install --prefix=#{prefix} --install-scripts=#{scripts}"
      command "sed -i 's/\\#\\!\\/usr\\/bin\\/python/\\#\\!\\/opt\\/scalarizr\\/embedded\\/bin\\/python/1' #{install_dir}/scripts/*"
    end
end