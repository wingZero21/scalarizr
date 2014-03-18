import os

from fabric.api import env, cd, local, run, put, get
from fabric.context_managers import shell_env

project = os.environ['FAB_PROJECT']

build = os.environ['PWD'].split('-')[-1]
build_dir = os.environ['PWD']

version = local("git describe --tag", capture=True)
branch = local("git rev-parse --abbrev-ref HEAD", capture=True)

omnibus_dir = os.path.join(build_dir, 'omnibus')
omnibus_build_version = '%s.b%s' % (version, build[0:8])

artifacts_dir = os.environ['ARTIFACTS_DIR']


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
    # bump version
    with cd(build_dir):
        run("echo '%s' >src/%s/version" % (version, project))

    # build
    with cd(omnibus_dir):
        run("bundle install --binstubs")
        with shell_env(BUILD_DIR=build_dir, OMNIBUS_BUILD_VERSION=omnibus_build_version):
            run("bin/omnibus build project %s" % project)


def build_source():
    git_export()

    with cd(build_dir):
        # bump version
        run("echo '%s' >src/%s/version" % (version, project))
        # build
        run("python setup.py sdist")

    local("mkdir -p %s" % os.path.join(artifacts_dir, project, branch, build))
    get(
        '%s/dist/*.tar.gz' % build_dir,
        os.path.join(artifacts_dir, project, branch, build)
    )


def build_binary():
    git_export()
    build_omnibus()

    local("mkdir -p %s" % os.path.join(artifacts_dir, project, branch, build))
    files = run("ls %s/omnibus/pkg/*%s*" % (build_dir, omnibus_build_version)).split()
    for f in files:
        get(f, os.path.join(artifacts_dir, project, branch, build))
        run('rm -f /var/cache/omnibus/pkg/%s' % os.path.basename(f))
