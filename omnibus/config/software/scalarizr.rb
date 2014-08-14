name "scalarizr"

dependency "scalarizr-deps"

source :path => ENV['BUILD_DIR']

build do
  command "#{install_dir}/embedded/bin/python setup.py install " \
            "--prefix=#{install_dir}/embedded " \
            "--install-data #{install_dir} " \
            "--install-scripts #{install_dir}/bin"
end
