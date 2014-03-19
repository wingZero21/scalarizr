import os

from fabric.api import env, cd, local, run, put, get
from fabric.context_managers import shell_env


BUILD_DIR = os.environ['PWD']
PROJECT = os.environ['FAB_PROJECT']
ARTIFACTS_DIR = os.environ['ARTIFACTS_DIR']

GIT_TAG = local("git describe --abbrev=0 --tags", capture=True)
GIT_BRANCH=local("git rev-parse --abbrev-ref HEAD", capture=True)
GIT_REF=local("git rev-parse HEAD", capture=True)

BUILD = os.environ['PWD'].split('-')[-1]
VERSION = '%s.b%s.b%s' % (GIT_TAG, BUILD[0:8], GIT_REF[0:8])
OMNIBUS_DIR = os.path.join(BUILD_DIR, 'omnibus')
OMNIBUS_BUILD_VERSION = VERSION


def git_export():
    archive = '/tmp/%s.tar.gz' % BUILD

    local("git archive --format=tar HEAD | gzip >%s" % archive)
    put(archive, archive)
    local("rm -f %s" % archive)

    run("mkdir -p %s" % BUILD_DIR)
    with cd(BUILD_DIR):
        run("tar -xf %s" % archive)
    run("rm -f %s" % archive)


def build_omnibus():
    # bump project version
    with cd(BUILD_DIR):
        run("echo '%s' >src/%s/version" % (VERSION, PROJECT))

    # build project
    with cd(OMNIBUS_DIR):
        run("bundle install --binstubs")
        with shell_env(BUILD_DIR=BUILD_DIR, OMNIBUS_BUILD_VERSION=OMNIBUS_BUILD_VERSION):
            run("bin/omnibus build project %s" % PROJECT)


def build_source():
    git_export()

    with cd(BUILD_DIR):
        # bump project version
        run("echo '%s' >src/%s/version" % (VERSION, PROJECT))
        # build project
        run("python setup.py sdist")

    local("mkdir -p %s" % os.path.join(ARTIFACTS_DIR, PROJECT, BRANCH, BUILD))
    get(
        '%s/dist/*.tar.gz' % BUILD_DIR,
        os.path.join(ARTIFACTS_DIR, PROJECT, BRANCH, BUILD)
    )


def build_binary():
    git_export()
    build_omnibus()

    local("mkdir -p %s" % os.path.join(ARTIFACTS_DIR, PROJECT, BRANCH, BUILD))
    files = run("ls %s/omnibus/pkg/*%s*" % (BUILD_DIR, OMNIBUS_BUILD_VERSION)).split()
    for f in files:
        get(f, os.path.join(ARTIFACTS_DIR, PROJECT, BRANCH, BUILD))
        run('rm -f /var/cache/omnibus/pkg/%s' % os.path.basename(f))
