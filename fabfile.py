import os
from fabric.api import *

env.user = 'root'

build = os.environ["PWD"].split('-')[-1]
build_dir = os.path.join('/root/ci/build', build)


def git_export():
    archive = '/tmp/%s.tar.gz' % build

    local("git archive --format=tar HEAD | gzip >%s" % archive)
    put(archive, archive)
    local("rm -f %s" % archive)

    run("mkdir -p %s" % build_dir)
    with cd(build_dir):
        run("tar -xf %s" % archive)
    run("rm -f %s" % archive)


def omnibus_build():
    omnibus_dir = os.path.join(build_dir, 'omnibus')
    with cd(omnibus_dir):
        run("bundle install --binstubs")
        run("bin/omnibus build project scalarizr")


def build_source():
    git_export()
    branch = local("git rev-parse --abbrev-ref HEAD", capture=True)
    version = local("git describe --tag", capture=True)
    with cd(build_dir):
        # bump version
        run("echo '%s' >src/scalarizr/version" % version)
        # build
        run("python setup.py sdist")
    local("mkdir /root/ci/artifacts/%s" % build)
    get('%s/dist/*.tar.gz' % build_dir, '/root/ci/artifacts/%s' % build)
