# pylint: disable=W0614
import os
import re
import time
import glob

from fabric.api import *
from fabric.decorators import runs_once
from fabric.context_managers import shell_env
from fabric.colors import green


project = os.environ.get('CI_PROJECT', 'scalarizr')
build_dir = os.environ['PWD']
home_dir = os.environ.get('CI_HOME_DIR', '/var/lib/ci')
omnibus_dir = os.path.join(build_dir, 'omnibus')
project_dir = os.path.join(home_dir, project)
build_number_file = os.path.join(project_dir, '.build_number')
omnibus_md5sum_file = os.path.join(project_dir, '.omnibus.md5')
build_number = None
artifacts_dir = None
tag = None
branch = None
version = None
repo = None
rubies = '/opt/rubies/ruby-2.1.1/bin'


current_contents = None
with open('/etc/environment', 'r+') as fp:
    current_contents = fp.read()
with open('/etc/environment', 'w+') as fp:
    result = ''
    if 'PATH' in current_contents:
        for line in current_contents.splitlines():
            if 'PATH' in line and rubies not in line:
                path = line.split('=')[1].strip()
                line = ''.join(['PATH="', line, ':', rubies, '"'])
            result += line
    else:
        result = current_contents + '\nPATH="{0}:{1}"'.format(os.environ['PATH'], rubies)
    fp.write(result)


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

    print_green('build_number: {0}'.format(build_number))
    print_green('artifacts_dir: {0}'.format(artifacts_dir))


@runs_once
def init():
    '''
    Initialize current build.
    '''
    global tag, branch, version, repo, build_number, artifacts_dir

    build_number = read_build_number()
    print_green('build_number: {0}'.format(build_number))
    setup_artifacts_dir()

    revision = local("git rev-parse HEAD", capture=True)
    ref_head = open('.git/HEAD').read().strip()
    if ref_head == revision:
        ref = re.search(r'moving from ([^\s]+) to [0-9a-f]{8,40}',
                        local('git reflog', capture=True), re.M).group(1)
    else:
        ref = ref_head.split('refs/heads/')[-1]
    is_tag = 'refs/tags/' in local('git show-ref {0}'.format(ref), capture=True)
    pkg_version = local('python setup.py --version', capture=True)
    if is_tag:
        # it's a tag
        tag = version = ref
        repo = 'stable' if int(tag.split('.')[1]) % 2 else 'latest'
        print_green('tag & version: {0}'.format(tag))
    else:
        # it's a branch
        branch = ref.replace('/', '-').replace('_', '-').replace('.', '')
        env.branch = branch
        version = '{version}.b{build_number}.{revision}'.format(
            version=pkg_version,
            build_number=build_number,
            revision=revision[0:8])
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
    files = get(src, artifacts_dir)
    print_green('imported artifacts: {0!r}'.format(
        [os.path.basename(f) for f in files]))


def git_export():
    '''
    Export current git tree to slave server into the same directory name
    '''
    print_green('in git export')
    archive = '%s.tar.gz' % project
    local("git archive --format=tar HEAD | gzip >%s" % archive)
    if not os.path.exists(archive):
        f = open(arhive, 'w+')
        f.close()
    if '.strider' in build_dir:
        build_dir_pattern = build_dir.rsplit('-', 1)[0] + '-*'
        if os.path.exists(build_dir_pattern):
            run("rm -rf {0}".format(build_dir_pattern))
    elif os.path.exists(build_dir):
        run("rm -rf %s" % build_dir)
    run("mkdir -p %s" % build_dir)
    put(archive, build_dir)
    if os.path.exists(archive):
        local('rm -f %s' % archive)
    print_green('build_dir is %s ' % build_dir)
    with cd(build_dir):
        run("tar -xf %s" % archive)


def local_export():
    '''
    Export current working copy to slave server into the same directory
    '''
    # TODO: merge with git_export
    archive = '%s.tar.gz' % project
    local("tar -czf %s ." % archive)
    run("rm -rf %s" % build_dir)
    run("mkdir -p %s" % build_dir)
    put(archive, build_dir)
    local('rm -f %s' % archive)

    with cd(build_dir):
        run("tar -xf %s" % archive)


def build_omnibus():
    # rm old installation
    print_green('building omnibus with dependencies')
    run("rm -rf /opt/%s" % project)
    with cd(omnibus_dir):
        run("[ -f bin/omnibus ] || bundle install --binstubs")
        env = {
            'BUILD_DIR': build_dir,
            'OMNIBUS_BUILD_DEPS': '1',
            'OMNIBUS_BUILD_VERSION': version,
        }
        with shell_env(**env):
            run("bin/omnibus clean %s" % project)
            run("bin/omnibus build project %s --log-level=info" % project)

    with open(omnibus_md5sum_file, 'w+') as fp:
        fp.write(omnibus_md5sum())


@task
def build_source():
    '''
    create source distribution
    '''
    init()
    git_export()
    with cd(build_dir):
        # bump project version
        run("echo {0!r} >version".format(version))
        # build project
        run("python setup_agent.py sdist", quiet=True)
        # import tarball
        import_artifact('dist/*.tar.gz')


@task
def build_binary():
    '''
    create binary distribution (.deb .rpm)
    '''
    init()
    git_export()
    generate_changelog()

    build_omnibus()
    import_artifact('/var/cache/omnibus/pkg/{0}*'.format(project))


def omnibus_md5sum_changed():
    if not os.path.exists(omnibus_md5sum_file):
        return True
    md5_old = open(omnibus_md5sum_file).read()
    md5_new = omnibus_md5sum()
    return md5_old != md5_new


def omnibus_md5sum():
    return local("find 'omnibus' -type f | sort | xargs md5sum", capture=True).strip()


def generate_changelog():
    # pylint: disable=W0612,W0621
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
    local('aptly repo remove {0} {1}'.format(repo, project))
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
    try:
        arch = 'i386' if env.host_string.endswith('32') else 'x86_64'
        remote_source = '/var/cache/omnibus/pkg/{0}*.rpm'.format(project)
        host_dest_str = '/var/www/rpm/%s/rhel/{alias}/%s'
        hds_cp = host_dest_str % (branch, arch + '/')  # host destination string for copying
        hds_ln = host_dest_str % (branch + '/', '')  # host destination string for symlinks
        host_destination = hds_cp.format(alias='5')
        # remove previous packages
        if os.path.exists(host_destination):
            local('rm -rf {0}'.format(host_destination))

        # download package file from remote to host machine
        get(remote_source, host_destination + '%(path)s')

        # create symlink for 5server
        if not os.path.exists(hds_ln.format(alias='5server')):
            os.makedirs(hds_ln.format(alias='5server'))
            local(
                'ln -s {five} {fivesrv}'.format(
                    five=host_destination,
                    fivesrv=hds_ln.format(alias='5server')
                )
            )
        # on host copy contents for 5 into 6
        # and create repo in six
        if not os.path.exists(hds_cp.format(alias='6')):
            os.makedirs(hds_cp.format(alias='6'))
        local(
            'cp -r {five} {six}; createrepo {six}'.format(
                five=host_destination,
                six=hds_cp.format(alias='6')
            )
        )
        # now it's ok to create repo in 5
        local('createrepo {five}'.format(five=host_destination))

        # create symlinks for alternate 6 versions
        for alias in ('6Server', '6.0', '6.1',
                      '6.2', '6.3', '6.4', '6.5', 'latest',):
            destination = hds_ln.format(alias=alias)
            if not os.path.exists(destination):
                os.makedirs(hds_ln.format(alias=alias))
                local('ln -s {source} {dest}'.format(
                    source=host_destination, dest=destination)
                )

        with cd(host_destination):
            local('createrepo {0}'.format(host_destination))
    finally:
        # cleanup contents of remote .rpm source
        run("rm -rf /var/cache/omnibus/pkg/*")


@task
@runs_once
def release(repo='latest'):
    '''
    sync packages from local repository to Scalr.net
    '''
    pass


@task
@runs_once
def publish_binary():
    '''
    Create .deb or .rpm binary according to current host name.
    '''
    if 'centos' in env.host_string:
        publish_rpm()
    else:
        publish_deb()


@task
def build_and_publish_binary():
    """
    Build and publish an approptiate binary package.
    """
    build_binary()
    publish_binary()


def print_green(msg):
    print green('[localhost] {0}'.format(msg))
