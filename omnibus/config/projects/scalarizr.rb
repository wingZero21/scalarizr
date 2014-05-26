
name       "scalarizr"
maintainer "Scalr Inc"
homepage   "http://scalr.com"

replaces        "scalarizr"
install_path    "/opt/scalarizr"
if ENV['OMNIBUS_BUILD_VERSION']
    build_version   ENV['OMNIBUS_BUILD_VERSION']
else
    build_version   1 #Omnibus::BuildVersion.new.semver
end
build_iteration 1

# creates required build directories
dependency "preparation"

if ENV['OMNIBUS_BUILD_BASE'] == 'y'
    dependency "scalarizr-deps"
else
    dependency "scalarizr"
    extra_package_file("--deb-changelog '#{Omnibus::Config.project_root}/changelog'")
end

# version manifest file
dependency "version-manifest"

exclude "\.git*"
exclude "bundler\/git"
