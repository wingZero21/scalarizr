name "scalarizr-deps"

default_version   "0.1"

if windows?
  dependency "python-windows"
end
if linux?
  dependency "rsync"
  dependency "sqlite3"
  dependency "bzip2"
  dependency "python"
end

dependency "pip"
dependency "cloudstack-python-client"
dependency "google-api-python-client"
dependency "python-boto"
dependency "python-cinderclient"
dependency "python-cloudfiles"
dependency "python-cloudservers"
dependency "python-cryptography"
dependency "python-docopt"
dependency "python-keystoneclient"
dependency "python-novaclient"
dependency "python-pexpect"
dependency "python-prettytable"
dependency "python-pychef"
dependency "python-pymongo"
dependency "python-pymysql"
dependency "python-pysnmp"
dependency "python-pysnmp-mibs"
dependency "python-pyyaml"
dependency "python-rackspace-novaclient"
dependency "python-simplejson"
dependency "python-swiftclient"
