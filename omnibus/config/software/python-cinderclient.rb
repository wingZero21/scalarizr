name "python-cinderclient"
pypi_name = "python-cinderclient"
default_version "1.0.5"

dependency "pip"
dependency "lxml"

if windows?
  revision =  "8e87c0b600d0742fbdd69ab770dbeb9dca7cf58c"
  pip = "#{install_dir}/embedded/python/Scripts/pip.exe"
  git_repo = 'https://github.com/openstack/python-cinderclient.git'
  build do
    # preinstall python keystoneclient
    command "#{pip} install -I git+https://github.com/openstack/python-keystoneclient.git"
    #install cinderclient, don't ignore already installed packages 
    command "#{pip} install git+#{git_repo}@#{revision}"
  end
else
  pip = "#{install_dir}/embedded/bin/pip"
  build do
    command "#{pip} install #{pypi_name}==#{default_version}"
  end
end

