import os
import cStringIO
from fabric.api import *

env.user = 'root'
env.key_filename = '/Users/marat/.keys/5071.pem'

def _target(hostname=None, keyname=None):
	if hostname and keyname:
		env.host_string = hostname
		for place in ('~/keys', '~/Workspace/login', "~/Downloads"):
			keyfile = os.path.expanduser(place + '/' + keyname)
			if os.path.exists(keyfile):
				env.key_filename = keyfile
				break


def upload(hostname=None, keyname=None):
	# fab upload:hostname=23.20.29.154,keyname=vova-percona-centos6-devel.pem
	_target(hostname, keyname)
	local('rm -f /tmp/scalarizr-0.9.tar.gz')
	local('tar -czf /tmp/scalarizr-0.9.tar.gz .')
	run('rm -rf /root/scalarizr')
	run('mkdir -p /root/scalarizr')
	put('/tmp/scalarizr-0.9.tar.gz', '/root/scalarizr')
	run('tar -xzf /root/scalarizr/scalarizr-0.9.tar.gz -C /root/scalarizr')
	setup_tests_deps()


def setup_tests_deps(hostname=None, keyname=None):
	_target(hostname, keyname)
	with settings(warn_only=True):
		if run('cat /etc/*-release | head -1 | grep -q Ubuntu').succeeded:
			run('which easy_install || apt-get install python-setuptools')
			run("which pip || apt-get install python-pip")
			run("which git || apt-get install -y git")
			run("which lettuce || pip install git+https://github.com/Scalr/lettuce.git")
		else:
			run('which easy_install || yum install python-setuptools')
			run("which pip-python || yum install python-pip")
			run("which git || yum install git")
			run("which lettuce || pip-python install git+https://github.com/Scalr/lettuce.git")
	run('easy_install mock nose')


def apt_update_from(branch):
	release = branch.replace('/', '-')
	deb_source = cStringIO.StringIO('deb http://buildbot.scalr-labs.com/apt/debian {release}/'.format(**locals()))
	put(deb_source, '/etc/apt/sources.list.d/scalr-stable.list')
	put(deb_source, '/etc/apt/sources.list.d/scalr-latest.list')

	apt_prefs = cStringIO.StringIO((
		'Package: scalarizr-base\n'
		'Pin: release {release}\n'
		'Pin-Priority: 990\n'
		'\n'
		'Package: scalarizr-ec2\n'
		'Pin: release {release}\n'
		'Pin-Priority: 990\n'
	).format(**locals()))
	put(apt_prefs, '/etc/apt/preferences.d/scalr')

	run('apt-get update')

