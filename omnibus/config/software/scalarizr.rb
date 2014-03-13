name "scalarizr"
version ENV['SCALARIZR_VERSION'] or "0.21.17"

dependency "python"

source :git => "git@github.com:Scalr/int-scalarizr.git"

relative_path "int-scalarizr"

build do
  FileUtils.mkdir_p("/etc/scalr")
  command "#{install_dir}/embedded/bin/python setup.py install --prefix=#{install_dir}/embedded"
end
