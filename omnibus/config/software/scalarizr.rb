name "scalarizr"

dependency "scalarizr-deps"

source :path => ENV['BUILD_DIR']

build do
  command "#{install_dir}/embedded/bin/python setup_omnibus.py install " \
            "--prefix=#{install_dir}/embedded " \
            "--install-scripts #{install_dir}/bin"
  command "sed -i 's/\\#\\!\\/usr\\/bin\\/python/\\#\\!\\/opt\\/scalarizr\\/embedded\\/bin\\/python/1' #{install_dir}/scripts/*"
end
