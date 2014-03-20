
name       "scalarizr"
maintainer "Scalr Inc"
homepage   "http://scalr.com"

extra_package_files(["--deb-changelog '/tmp/changelog'"])

replaces        "scalarizr"
install_path    "/opt/scalarizr"
if ENV['OMNIBUS_BUILD_VERSION']
    build_version   ENV['OMNIBUS_BUILD_VERSION']
else
    build_version   Omnibus::BuildVersion.new.semver
end
build_iteration 1

# creates required build directories
dependency "preparation"

# scalarizr dependencies/components
dependency "scalarizr-deps"
dependency "scalarizr"

# version manifest file
dependency "version-manifest"

exclude "\.git*"
exclude "bundler\/git"
