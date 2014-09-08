name "gdbm"
default_version "1.8.3-1"

dependency "pip"


source :url => "http://downloads.sourceforge.net/project/gnuwin32/gdbm/#{default_version}/gdbm-#{default_version}.exe?r=&ts=1410156881&use_mirror=dfn",
       :md5 => 'c8dc73944363ac2215b2bf218a0e0211'
if windows?
    package_src = "C:/omnibus-ruby/src/gdbm/gdbm-#{default_version}.exe"
    build do  
      command "call #{windows_safe_path(package_src)} /sp /verysilent /suppressmsgboxes"
    end
end