import os

from fabric.api import env, cd, local, run, put, get
from fabric.context_managers import shell_env


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


def build_omnibus():
    omnibus_dir = os.path.join(build_dir, 'omnibus')
    version = local("git describe --tag", capture=True)
    omnibus_build_version = '%s.b%s' % (version, build[0:8])
    with cd(omnibus_dir):
        run("bundle install --binstubs")
        with shell_env(BUILD_DIR=build_dir, OMNIBUS_BUILD_VERSION=omnibus_build_version):
            run("bin/omnibus build project scalarizr")


def build_source():
    git_export()
    version = local("git describe --tag", capture=True)
    with cd(build_dir):
        # bump version
        run("echo '%s' >src/scalarizr/version" % version)
        # build
        run("python setup.py sdist")
    local("mkdir /root/ci/artifacts/%s" % build)
    get('%s/dist/*.tar.gz' % build_dir, '/root/ci/artifacts/%s' % build)


def build_binary():
    git_export()
    build_omnibus()
    version = local("git describe --tag", capture=True)
    omnibus_build_version = '%s.b%s' % (version, build[0:8])
    files = run("ls %s/omnibus/pkg/*%s*" % (build_dir, omnibus_build_version)).split()
    for f in files:
        get(f, '/root/ci/repos/%s' % os.path.basename(f))
        run('rm -f /var/cache/omnibus/pkg/%s' % os.path.basename(f))


def clean():
    run("rm -rf %s" % build_dir)
