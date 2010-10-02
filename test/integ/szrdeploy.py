#!/usr/bin/python

import sys, os
from optparse import OptionParser
import logging
import shutil

pwd = os.path.dirname(__file__)
try:
	import scalarizr
except ImportError:
	sys.path.append(os.path.abspath(pwd  + '/../../src'))
	import scalarizr

try:
	import szr_integtest_libs, szr_integtest
except ImportError:
	sys.path.append(pwd + '/libs')
	sys.path.append(pwd + '/testcases')
	import szr_integtest_libs, szr_integtest

from scalarizr.util import system
from szr_integtest_libs import SshManager
from szr_integtest_libs.szrdeploy import RepoType, ScalarizrDeploy


# Command-line options 
parser = OptionParser()
parser.add_option('--host', dest='host', action='store', help='Server host')
parser.add_option('-i', '--key', dest='key', action='store', help='SSH private key path')

cmds = parser.add_option_group('Installation commands')
cmds.add_option('--nightly', dest='nightly', action='store_true', help='Install nightly build')
cmds.add_option('--rc', dest='rc', action='store_true', help='Install release candidate')
cmds.add_option('--release', dest='release', action='store_true', help='Install release')

cmds2 = parser.add_option_group('Update commands')
cmds2.add_option('-u', '--update-package', dest='update_package', action='store_true', help='Update package')
cmds2.add_option('-s', '--update-from-svn', dest='update_from_svn', action='store_true', help='Update files with latest from SVN')
cmds2.add_option('-l', '--update-from-lc', dest='update_from_lc', action='store_true', help='Update files from local copy')

parser.parse_args()
vals = parser.values
if not (vals.nightly or vals.rc or vals.release or \
		vals.update_package or vals.update_from_svn or vals.update_from_lc):
	parser.print_help()
	sys.exit(1)
if not vals.host:
	print 'host is required'
	sys.exit(1)
if not vals.key:
	print 'ssh private key is required'
	sys.exit(1)

# Init logging	
logging.basicConfig(
		format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
		stream=sys.stdout, 
		level=logging.INFO)
logger = logging.getLogger('deploy')

	
# Deploy scalarizr
logger.info('Connecting to server %s', vals.host)
ssh = SshManager(vals.host, vals.key)
ssh.connect()
deploy = ScalarizrDeploy(ssh.get_root_ssh_channel())


if vals.nightly or vals.rc or vals.release:
	logger.info('Add repos')
	deploy.add_repos(
		(vals.nightly and RepoType.NIGHTLY_BUILD) or \
		(vals.rc and RepoType.RC) or \
		(vals.release and RepoType.RELEASE) 
	)
	
	logger.info('Install scalarizr')
	deploy.install_package()

elif vals.update_package:
	logger.info('Update scalarizr')
	deploy.update_package()
	
elif vals.update_from_svn:
	logger.info('Updating files from SVN')
	deploy.apply_changes_from_svn()
elif vals.update_from_lc:
	logger.info('Updating files from local copy')
	tarball  = '/tmp/scalarizr.tar.gz'
	system('tar -czf %s -C %s src' % (tarball, os.path.abspath(pwd + '/../..')))
	deploy.apply_changes_from_tarball(tarball)
	os.remove(tarball)
	
else:
	logger.info('nothing to do')
	
logger.info('Done')