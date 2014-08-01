
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

# creates required build directories
dependency "preparation"


dependency "scalarizr"


# version manifest file
dependency "version-manifest"
extra_package_file("--deb-changelog '#{Omnibus::Config.project_root}/changelog'")

exclude "\.git*"
exclude "bundler\/git"
