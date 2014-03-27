import os
import time

from fabric.api import env, cd, local, run, put, get
from fabric.context_managers import shell_env


BUILD_DIR = os.environ['PWD']
PROJECT = os.environ['FAB_PROJECT']

GIT_TAG = local("git describe --abbrev=0 --tags", capture=True)
GIT_BRANCH = local("git rev-parse --abbrev-ref HEAD", capture=True)
NRM_BRANCH = GIT_BRANCH.replace('/', '-').replace('_', '-').replace('.', '')
GIT_REF = local("git rev-parse HEAD", capture=True)

BUILD = os.environ['PWD'].split('-')[-1]
OMNIBUS_DIR = os.path.join(BUILD_DIR, 'omnibus')

if GIT_REF == open('.git/HEAD', 'r').read().strip():
    VERSION = GIT_TAG
    MASTER_ARTIFACTS_DIR = os.path.join(os.environ['CI_DIR'], 'artifacts', PROJECT, GIT_TAG, BUILD)
    SLAVE_ARTIFACTS_DIR = os.path.join('/vagrant', 'artifacts', PROJECT, GIT_TAG, BUILD)
else:
    VERSION = '%s.b%s.%s' % (GIT_TAG, BUILD[0:8], GIT_REF[0:8])
    MASTER_ARTIFACTS_DIR = os.path.join(os.environ['CI_DIR'], 'artifacts', PROJECT, NRM_BRANCH, BUILD)
    SLAVE_ARTIFACTS_DIR = os.path.join('/vagrant', 'artifacts', PROJECT, NRM_BRANCH, BUILD)

OMNIBUS_BUILD_VERSION = VERSION


def git_export():
    archive = '%s.tar.gz' % PROJECT
    local("git archive --format=tar HEAD | gzip >%s" % archive)
    run("mkdir -p %s" % BUILD_DIR)
    put(archive, BUILD_DIR)
    with cd(BUILD_DIR):
        run("tar -xf %s" % archive)


def import_source():
    local("mkdir -p %s" % MASTER_ARTIFACTS_DIR)
    run("mv %s/dist/*.tar.gz %s" % (BUILD_DIR, SLAVE_ARTIFACTS_DIR))


def import_binary():
    local("mkdir -p %s" % MASTER_ARTIFACTS_DIR)
    files = run("ls %s/omnibus/pkg/*%s*" % (BUILD_DIR, OMNIBUS_BUILD_VERSION)).split()
    for f in files:
        run("mv %s %s" % (f, SLAVE_ARTIFACTS_DIR))
        run('rm -f /var/cache/omnibus/pkg/%s' % os.path.basename(f))


def build_omnibus():
    # bump project version
    with cd(BUILD_DIR):
        run("echo '%s' >src/%s/version" % (VERSION, PROJECT))

    # build project
    with cd(OMNIBUS_DIR):
        run("[ -f bin/omnibus ] || bundle install --binstubs")
        env = {
            'BUILD_DIR': BUILD_DIR,
            'OMNIBUS_BUILD_VERSION': OMNIBUS_BUILD_VERSION,
        }
        if 'BUILD_DEPENDENCY' in os.environ:
            env.update({'BUILD_DEPENDENCY': os.environ['BUILD_DEPENDENCY']}) 
        with shell_env(**env):
            run("bin/omnibus build project --without-healthcheck %s" % PROJECT)


def build_source():
    git_export()

    with cd(BUILD_DIR):
        # bump project version
        run("echo '%s' >src/%s/version" % (VERSION, PROJECT))
        # build project
        run("python setup.py sdist")

    import_source()


def build_binary():
    git_export()
    generate_changelog()
    build_omnibus()
    #changelog_workaround()
    import_binary()


def generate_changelog():
    template = \
    """{package} ({version}) {branch}; urgency=low

  * Build {package}

 -- {author} <{author_email}>  {now}"""
    
    package = PROJECT
    version = VERSION
    branch = NRM_BRANCH
    author = local("git show -s --format=%an", capture=True)
    author_email = local("git show -s --format=%ae", capture=True)
    now = time.strftime("%a, %d %b %Y %H:%M:%S %z", time.gmtime())
    with cd(OMNIBUS_DIR):
        run("echo '%s' >changelog" % template.format(**locals()))


def changelog_workaround():
    with cd(os.path.join(OMNIBUS_DIR, 'pkg')):
        run("mv *.deb tmp.deb")
        run("fpm -s deb -t deb --deb-changelog ../changelog -n %s tmp.deb" % PROJECT)
        run("rm -f tmp.deb")

 
def cleanup():
    run("rm -rf %s" % BUILD_DIR)
