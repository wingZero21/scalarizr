name "scalarizr"

dependency "python"

source :path => ENV['BUILD_DIR']

build do
  FileUtils.mkdir_p("/etc/scalr")
  command "#{install_dir}/embedded/bin/python setup.py install --prefix=#{install_dir}/embedded"
end
