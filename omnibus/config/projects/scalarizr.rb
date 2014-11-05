
name       "scalarizr"
maintainer "Scalr Inc"
homepage   "http://scalr.com"

install_dir    "/opt/scalarizr"

if ENV['OMNIBUS_BUILD_VERSION']
    build_version   ENV['OMNIBUS_BUILD_VERSION']
else
    build_version   Omnibus::BuildVersion.new.semver
end
build_iteration 1

if windows?
  package :msi do
    upgrade_code 'c8a28fde-f340-423f-b7db-ff1368d8bb19'
  end
end

dependency "preparation"
dependency "scalarizr"
dependency "version-manifest"

if ohai['platform_family'] == 'rhel'
    if ohai['platform_version'].to_i == 5
        runtime_dependency "yum-downloadonly"
        runtime_dependency "yum-priorities"
    else
        runtime_dependency "yum-plugin-downloadonly"
        runtime_dependency "yum-plugin-priorities"
    end
    runtime_dependency "which"
    runtime_dependency "e2fsprogs"
    runtime_dependency "tar"
end 

exclude "\.git*"
exclude "bundler\/git"
