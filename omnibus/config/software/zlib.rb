name "zlib"
default_version "2.2"

dependency "pip"

if windows?
    project_root = "#{ENV['TMP']}/bootstrap/int-scalarizr"
    binary_location = "#{project_root}/omnibus/files/curses-2.2.win-amd64-py2.7.exe"
    easy_install = "{#{install_dir}}/embedded/python/Scripts/easy_install.exe"
    prefix = "#{install_dir}/embedded/python/Lib/site-packages/"
    build do  
      command "#{windows_safe_path(easy_install)} --install-dir=#{windows_safe_path(prefix)} #{windows_safe_path(binary_location)}"
    end
end