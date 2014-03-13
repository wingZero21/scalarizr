
name "scalarizr"
maintainer "Scalr Inc"
homepage "http://scalr.com"

replaces        "scalarizr"
install_path    "/opt/scalarizr"
#if ENV['SCALARIZR_OMNIBUS_VERSION']
#    build_version   ENV['SCALARIZR_OMNIBUS_VERSION']
#else
#    build_version   Omnibus::BuildVersion.new.semver
#end
build_version   Omnibus::BuildVersion.new.semver
build_iteration 1

# creates required build directories
dependency "preparation"

# scalarizr dependencies/components
dependency "scalarizr-deps"
#dependency "scalarizr-update"
#dependency "scalarizr"

# version manifest file
dependency "version-manifest"

exclude "\.git*"
exclude "bundler\/git"
