import os
import time
import glob


from fabric.api import *
from fabric.decorators import runs_once
from fabric.context_managers import shell_env
from fabric.colors import green, red


project = os.environ.get('CI_PROJECT', 'scalarizr')
build_dir = os.environ['PWD']
home_dir = os.environ.get('CI_HOME_DIR', '/var/lib/ci')
omnibus_dir = os.path.join(build_dir, 'omnibus')
project_dir = os.path.join(home_dir, project)
repos_dir = os.path.join(home_dir, 'repos')
master_synced_dir = os.path.join(os.environ.get('CI_APP_DIR', '/vagrant'), 'synced')
slave_synced_dir = '/vagrant/synced'
build_number_file = os.path.join(project_dir, '.build_number')
gpg_secret_file = os.path.join(home_dir, '.gpg_secret')
build_number = None
artifacts_dir = None
tag = None
branch = None
version = None
repo = None


def read_build_number():
    with open(build_number_file) as fp:
        return int(fp.read())


def setup_artifacts_dir():
    global artifacts_dir
    # append build_number to artifacts dir
    artifacts_dir = os.path.join(project_dir, str(build_number)) 


@task
def prepare():
    '''
    setup next build ('prepare' script in StriderCD)
    '''
    global artifacts_dir, build_number

    # setup project dir
    if not os.path.exists(project_dir):
        os.makedirs(project_dir)
    # bump build number
    if os.path.exists(build_number_file):
        build_number = read_build_number()
    else:
        build_number = 0
    build_number += 1   
    with open(build_number_file, 'w+') as fp:
        fp.write(str(build_number))
    setup_artifacts_dir()
    # setp artifacts dir
    if not os.path.exists(artifacts_dir):
        os.makedirs(artifacts_dir)
    # setup and clean synced dir
    if not os.path.exists(master_synced_dir):
        os.makedirs(master_synced_dir)
    local("rm -rf {0}/*".format(master_synced_dir))

    print_green('build_number: {0}'.format(build_number))
    print_green('artifacts_dir: {0}'.format(artifacts_dir))
    print_green('synced dirs: {0!r} (Master) -> {1!r} (Slave)'.format(
            master_synced_dir, slave_synced_dir))


@runs_once
def init():
    '''
    Initialize current build.
    '''
    global tag, branch, version, repo, build_number, artifacts_dir

    build_number = read_build_number()
    print_green('build_number: {0}'.format(build_number))
    setup_artifacts_dir()
    _tag = local("git describe --abbrev=0 --tags", capture=True)
    _branch = local("git rev-parse --abbrev-ref HEAD", capture=True)
    _branch = _branch.replace('/', '-').replace('_', '-').replace('.', '')
    _version = local('python setup.py --version', capture=True)
    _ref = local("git rev-parse HEAD", capture=True)[0:8]
    if _ref == open('.git/HEAD').read().strip():
        # it's a tag
        tag = version = _tag
        repo = 'stable' if int(tag.split('.')[1]) % 2 else 'latest'
        print_green('tag & version: {0}'.format(tag))
    else:
        # it's a branch
        branch = _branch
        version = '{version}.b{build_number}.{ref}'.format(
                    version=_version, build_number=build_number, ref=_ref)
        repo = branch
        print_green('branch: {0}'.format(branch))
        print_green('version: {0}'.format(version))
    print_green('repo: {0}'.format(repo))


def import_artifact(src):
    '''
    Utility function to import artifacts from Slave
    Example:

        with cd(build_dir):
            run('python setup.py sdist')
            import_artifact('dist/*')
    '''
    run("mv {0} {1}".format(src, slave_synced_dir))
    files = os.listdir(master_synced_dir)
    local("mv {0}/{1} {2}/".format(master_synced_dir, os.path.basename(src), artifacts_dir))
    print_green('imported artifacts: {0!r}'.format(files))


def git_export():
    '''
    Export current git tree to slave server into the same directory name
    '''
    archive = '%s.tar.gz' % project
    local("git archive --format=tar HEAD | gzip >%s" % archive)
    run("rm -rf %s" % build_dir)
    run("mkdir -p %s" % build_dir)
    put(archive, build_dir)
    with cd(build_dir):
        run("tar -xf %s" % archive)


def build_omnibus_base():
    # rm old installation
    run("rm -rf /opt/%s" % project)

    # rm cache
    run("rm -rf /var/cache/ci/%s" % project)

    # build base installation
    with cd(omnibus_dir):
        run("[ -f bin/omnibus ] || bundle install --binstubs")
        env = {
            'BUILD_DIR': build_dir,
            'OMNIBUS_BUILD_BASE': 'y',
        }
        with shell_env(**env):
            run("bin/omnibus clean %s" % project)
            run("bin/omnibus build project %s" % project)
            run("rm -rf /var/cache/omnibus/pkg/*")

    # save to cache
    run("mkdir -p /var/cache/ci")
    run("mv /opt/%s /var/cache/ci/%s" % (project, project))


def build_omnibus():
    # rm old installation
    run("rm -rf /opt/%s" % project)

    # copy base installation
    run("cp -r /var/cache/ci/%s /opt/" % project)

    # bump project version
    with cd(build_dir):
        run("echo '%s' >src/%s/version" % (version, project))

    # build project
    with cd(omnibus_dir):
        run("[ -f bin/omnibus ] || bundle install --binstubs")
        env = {
            'BUILD_DIR': build_dir,
            'OMNIBUS_BUILD_VERSION': version,
        }
        with shell_env(**env):
            run("bin/omnibus build project --without-healthcheck %s" % project)


@task
def build_source():
    '''
    create source distribution
    '''
    init()
    git_export()
    with cd(build_dir):
        # bump project version
        run("echo {0!r} >src/{1}/version".format(version, project))
        # build project
        run("python setup.py sdist", quiet=True)
        # import tarball    
        import_artifact('dist/*.tar.gz')


@task
def build_binary_base():
    '''
    create binary distribution base (execute once before 'build_binary' and when requirements.txt changed)
    '''
    init()
    git_export()
    build_omnibus_base()


@task
def build_binary():
    '''
    create binary distribution (.deb .rpm)
    '''
    init()
    git_export()
    generate_changelog()
    build_omnibus()
    import_artifact('/var/cache/omnibus/pkg/*')



def generate_changelog():
    template = \
    """{project} ({version}) {branch}; urgency=low

  * Build {project}

 -- {author} <{author_email}>  {now}"""
    
    project = globals()['project']
    version = globals()['version']
    branch = globals()['branch']
    author = local("git show -s --format=%an", capture=True)
    author_email = local("git show -s --format=%ae", capture=True)
    now = time.strftime("%a, %d %b %Y %H:%M:%S %z", time.gmtime())
    with cd(omnibus_dir):
        run("echo '%s' >changelog" % template.format(**locals()))


@task
@runs_once
def publish_deb():
    '''
    publish .deb packages into local repository
    '''
    init()
    if repo not in local('aptly repo list', capture=True):
        local('aptly repo create -distribution {0} {0}'.format(repo))
    local('aptly repo add {0} {1}'.format(
            repo, ' '.join(glob.glob(artifacts_dir + '/*.deb'))))
    if repo in local('aptly publish list', capture=True):
        local('aptly publish drop {0}'.format(repo))
    local('aptly publish repo {0}'.format(repo))


@task
@runs_once
def publish_rpm():
    '''
    publish .rpm packages into local repository
    '''
    pass


def release():
    pass


def cleanup():
    run("rm -rf %s" % build_dir)


def print_green(msg):
    print green('[localhost] {0}'.format(msg))

