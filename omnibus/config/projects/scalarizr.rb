
name       "scalarizr"
maintainer "Scalr Inc"
homepage   "http://scalr.com"

replaces        "scalarizr"
install_path    "/opt/scalarizr"
if ENV['OMNIBUS_BUILD_VERSION']
    build_version   ENV['OMNIBUS_BUILD_VERSION']
else
    build_version   Omnibus::BuildVersion.new.semver
end
build_iteration 1

extra_package_files(["--deb-changelog '#{Omnibus::Config.poject_root}/changelog'"])

# creates required build directories
dependency "preparation"

# scalarizr dependencies/components
dependency "scalarizr-deps"
dependency "scalarizr"

# version manifest file
dependency "version-manifest"

exclude "\.git*"
exclude "bundler\/git"
