name "scalarizr-update"
version "0.4"

dependency "python"

source = "git+git@github.com:Scalr/upd.git#egg=scalr-upd-client"

build do
  command "#{install_dir}/embedded/bin/pip install -I --build #{project_dir} -e #{source}"
end
