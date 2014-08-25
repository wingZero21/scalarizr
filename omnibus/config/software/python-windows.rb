name "python-windows"
default_version "2.7.8"

source :url => "http://www.python.org/ftp/python/#{version}/python-#{version}.amd64.msi",
       :md5 => '38cadfcac6dd56ecf772f2f3f14ee846'

relative_path "Python-#{version}"

build do
  block do
    FileUtils.mkdir_p(File.expand_path("embedded/python", install_dir))
  end
  target_dir = File.join(install_dir, "embedded", "python").gsub!('/', '\\')
  command "msiexec /a python-#{version}.amd64.msi /qb TARGETDIR=#{target_dir}"
end
