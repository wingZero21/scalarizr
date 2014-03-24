
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

extra_package_files(["--deb-changelog '#{Omnibus::Config.project_root}/changelog'"])

# creates required build directories
dependency "preparation"

if ENV['SCALARIZR_DEPENDENCY']
    # scalarizr dependencies/components
    dependency "scalarizr-deps"
end
dependency "scalarizr"

# version manifest file
dependency "version-manifest"

exclude "\.git*"
exclude "bundler\/git"
