# pylint: disable=W0614
import os
import re
import time
import glob
import json
import shutil

from fabric.api import *
from fabric.decorators import runs_once
from fabric.context_managers import shell_env
from fabric.colors import green, red

env['use_ssh_config'] = True
project = os.environ.get('CI_PROJECT', 'scalarizr')
build_dir = os.environ['PWD']
home_dir = os.environ.get('CI_HOME_DIR', '/var/lib/ci')
omnibus_dir = os.path.join(build_dir, 'omnibus')
project_dir = os.path.join(home_dir, project)
repo_dir = '/var/www'
aptly_conf = None
gpg_key = '04B54A2A'
build_number_file = os.path.join(project_dir, '.build_number')
omnibus_md5sum_file = os.path.join(project_dir, '.omnibus.md5')
permitted_artifacts_number = 2
build_number = None
artifacts_dir = None
tag = None
branch = None
version = None
repo = None


def read_build_number():
    print_green('Setting up artifacts dir')
    with open(build_number_file) as fp:
        return int(fp.read())


def setup_artifacts_dir():
    global artifacts_dir
    # append build_number to artifacts dir
    artifacts_dir = os.path.join(project_dir, str(build_number))
    local('mkdir -p {0}'.format(artifacts_dir))


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
    cleanup_artifacts()
    print_green('build_number: {0}'.format(build_number))
    print_green('artifacts_dir: {0}'.format(artifacts_dir))


@runs_once
def init():
    '''
    Initialize current build.
    '''
    global tag, branch, version, repo, build_number, artifacts_dir, aptly_conf

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
    # Load aptly.conf
    for aptly_conf_file in ('/etc/aptly.conf', os.path.expanduser('~/.aptly.conf')):
        if os.path.exists(aptly_conf_file):
            aptly_conf = json.load(open(aptly_conf_file))


    print_green('repo: {0}'.format(repo))


def import_artifact(src):
    '''
    Utility function to import artifacts from Slave
    Example:

        with cd(build_dir):
            run('python setup.py sdist')
            import_artifact('dist/*')
    '''
    print_green('importing artifacts from {0} to {1}'.format(src, artifacts_dir))

    files = get(src, artifacts_dir)
    print_green('imported artifacts:')
    for f in files:
        print_green(os.path.basename(f))


@serial
def git_export():
    '''
    Export current git tree to slave server into the same directory name
    '''
    try:
        host_str = env.host_string.split('@')[1]
    except IndexError:
        host_str = env.host_string
    archive = '{0}-{1}.tar.gz'.format(project, host_str)  # add host str, for safe concurrent execution
    local("git archive --format=tar HEAD | gzip >{0}".format(archive))
    if not os.path.exists(archive):
        f = open(archive, 'w+')
        f.close()
    if '.strider' in build_dir:
        build_dir_pattern = build_dir.rsplit('-', 1)[0] + '-*'
        if os.path.exists(build_dir_pattern):
            run("rm -rf {0}".format(build_dir_pattern))
    elif os.path.exists(build_dir):
        run("rm -rf %s" % build_dir)
    run("mkdir -p %s" % build_dir)

    current_dir = os.path.dirname(os.path.abspath(__file__))
    archive_path = os.path.join(current_dir, archive)
    put(archive_path, build_dir)
    if os.path.exists(archive):
        local('rm -f %s' % archive)
    print_green('exported git tree into %s on slave' % build_dir)
    with cd(build_dir):
        run("tar -xf %s" % archive)


def local_export():
    '''
    Export current working copy to slave server into the same directory
    '''
    archive = '{0}-{1}.tar.gz'.format(project, env.host_string)  # add host str, for safe concurrent execution
    local("tar -czf %s ." % archive)
    run("rm -rf %s" % build_dir)
    run("mkdir -p %s" % build_dir)
    put(archive, build_dir)
    local('rm -f %s' % archive)

    with cd(build_dir):
        run("tar -xf %s" % archive)


def build_omnibus():
    # rm old installation
    print_green('building omnibus')
    with cd(omnibus_dir):
        run("[ -f bin/omnibus ] || bundle install --binstubs")
        env = {
            'BUILD_DIR': build_dir,
            'OMNIBUS_BUILD_VERSION': version,
        }
        with shell_env(**env):
            run("bin/omnibus clean %s --log-level=warn" % project)
            run("bin/omnibus build %s --log-level=info" % project)

    with open(omnibus_md5sum_file, 'w+') as fp:
        fp.write(omnibus_md5sum())


def build_meta_packages():
    print_green('building meta packages')
    pkg_type = 'rpm' if 'centos' in env.host_string else 'deb'
    for platform in 'ec2 gce openstack cloudstack ecs idcf ucloud eucalyptus rackspace'.split():
        with cd('/var/cache/omnibus/pkg'):
            run(('fpm -t {pkg_type} -s empty '
                 '--name scalarizr-{platform} '
                 '--version {version} '
                 '--iteration 1 '
                 '--depends "scalarizr = {version}-1" '
                 '--maintainer "Scalr Inc. <packages@scalr.net>" '
                 '--url "http://scalr.net"').format(
                pkg_type=pkg_type, version=version,
                platform=platform))


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


def bump_version():
    with cd(build_dir):
        run("echo {0!r} >src/scalarizr/version".format(version))


@task
def build_binary():
    '''
    create binary distribution (.deb .rpm)
    '''
    time0 = time.time()
    init()
    git_export()
    bump_version()
    generate_changelog()
    run('rm -rf /var/cache/omnibus/pkg/{0}*'.format(project))
    build_omnibus()
    build_meta_packages()
    import_artifact('/var/cache/omnibus/pkg/{0}*'.format(project))
    time_delta = time.time() - time0
    print_green('build binary took {0}'.format(time_delta))


def omnibus_md5sum_changed():
    if not os.path.exists(omnibus_md5sum_file):
        return True

    with open(omnibus_md5sum_file) as fp:
        md5_old = fp.read()
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
    time0 = time.time()
    try:
        init()
        pkg_arch = 'i386' if env.host_string.endswith('32') else 'amd64'

        if repo not in local('aptly repo list', capture=True):
            local('aptly repo create -distribution {0} {0}'.format(repo))
        # remove previous version
        local('aptly repo remove {0} "Architecture ({1}), Name (~ {2}.*)"'.format(repo, pkg_arch, project))
        # publish artifacts into repo
        local('aptly repo add {0} {1}'.format(
            repo, ' '.join(glob.glob(artifacts_dir + '/*_{0}.deb'.format(pkg_arch)))))
        local('aptly publish drop {0} || :'.format(repo))
        local('aptly publish repo -gpg-key={1} {0} || :'.format(repo, gpg_key))
        local('aptly db cleanup')
    finally:
        time_delta = time.time() - time0
        print_green('publish deb took {0}'.format(time_delta))


@task
@runs_once
def publish_deb_plain():
    '''
    publish .deb packages into local repository as a plain debian repo (only for compatibility)
    '''
    init()

    with cd(aptly_conf['rootDir']):
        release_file = 'public/dists/{0}'.format(repo)
        arches = local('grep Architecture {0}'.format(release_file), 
                        capture=True).split(':')[-1].strip().split()
        repo_plain_dir = '{0}/apt-plain/{1}'.format(repo_dir, repo)
        if os.path.exists(repo_plain_dir):
            shutil.rmtree(repo_plain_dir)
        os.makedirs(repo_plain_dir)
        for arch in arches:
            packages_file = 'public/dists/{0}/main/binary-{1}/Packages'.format(repo, arch)
            # Copy packages
            local(("grep Filename %s | "
                    "awk '{ print $2 }' | "
                    "xargs cp -I '{}' cp '{}' %s/") % (packages_file, repo_plain_dir))

    with cd(os.path.dirname(repo_plain_dir)):
        local('dpkg-scanpackages -m {0} > {0}/Packages'.format(repo))
        local('dpkg-scansources {0} > {0}/Sources'.format(repo))
        with cd(repo):
            with open('Release' % repo) as fp:
                fp.write((
                    'Origin: scalr\n'
                    'Label: {0}\n'
                    'Codename: {0}\n'
                    'Architectures: all {1}\n'
                    'Description: Scalr packages\n'
                ).format(repo, ' '.join(arches)))
                fp.write(local('apt-ftparchive release .', capture=True))
            local('cat Packages | gzip -9c > Packages.gz')
            local('cat Sources | gzip -9c > Sources.gz')
            local('gpg -v --clearsign -u {0} -o InRelease Release'.format(gpg_key))
            local('gpg -v -abs -u {0} -o Release.gpg Release'.format(gpg_key))

    print_green('publish plain deb repository')

@task
@runs_once
def publish_rpm():
    '''
    publish .rpm packages into local repository.
    '''
    time0 = time.time()
    try:
        arch, pkg_arch = ('i386', 'i686') if env.host_string.endswith('32') else ('x86_64', 'x86_64')
        print_green('detected architecture: omnibus-naming - {0}, general-naming {1}'.format(pkg_arch, arch))
        repo_path = '%s/rpm/%s/rhel' % (repo_dir, repo)

        # create directory structure
        local('mkdir -p %s/{5,6,7}/{x86_64,i386}' % repo_path, shell='/bin/bash')
        cwd = os.getcwd()
        os.chdir(repo_path)

        def symlink(target, linkname):
            if not os.path.exists(linkname):
                os.symlink(target, linkname)
        for linkname in '5Server'.split():
            symlink('5', linkname)
        for linkname in '6Server 6.0 6.1 6.2 6.3 6.4 6.5'.split():
            symlink('6', linkname)
        for linkname in '7Server 7.0 latest'.split():
            symlink('7', linkname)
        os.chdir(cwd)

        # remove previous version
        local('rm -f %s/*/%s/%s*.rpm' % (repo_path, arch, project))

        # publish artifacts into repo
        for ver in '5 6 7'.split():
            dst = os.path.join(repo_path, ver, arch)
            local('cp %s/%s*%s.rpm %s/' % (artifacts_dir, project, pkg_arch, dst))
            local('createrepo %s' % dst)
    finally:
        time_delta = time.time() - time0
        print_green('publish rpm took {0}'.format(time_delta))




def cleanup_artifacts():
    print_green('Running cleanup task in {0}'.format(project_dir))
    artifact_dirs = sorted(glob.glob('{0}/*'.format(project_dir)))
    num_artifacts = len(artifact_dirs)
    if num_artifacts > permitted_artifacts_number:
        print_green(
            'Artifact number exeeding permitted value.'
            'Removing {0} artifact directories'.format(num_artifacts - permitted_artifacts_number))

        for directory in artifact_dirs[0:-permitted_artifacts_number]:
            if os.path.isdir(directory):
                local('rm -rf {0}'.format(directory))


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
        #publish_deb_plain()


@task
def build_and_publish_binary():
    """
    Build and publish an approptiate binary package.
    """
    time0 = time.time()
    try:
        build_binary()
        publish_binary()
    finally:
        run('rm -rf /root/.strider/data/scalr-int-scalarizr-*')
        run('find /tmp -mindepth 1 -maxdepth 1 ! -name "vagrant-chef-*" | xargs rm -rf')
        time_delta = time.time() - time0
        print_green('build_and_publish_binary took {0} minutes '.format(time_delta / 60))


def print_green(msg):
    print green('[localhost] {0}'.format(msg))


def print_red(msg):
    print red('[localhost] {0}'.format(msg))
