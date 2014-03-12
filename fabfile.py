import os
from fabric.api import *

env.user = 'root'

slaves = ['slave']


@hosts(slaves)
def git_export(project='scalarizr-omnibus'):
    project_path = '/root/ci/master/projects/%s' % project

    local("rm -f /tmp/%s.tar.gz" % project)
    local("cd %s && git archive --format=tar HEAD | gzip >/tmp/%s.tar.gz" % (project_path, project))

    run("rm -f /tmp/%s.tar.gz" % project)
    run("rm -rf /root/ci/slave/projects/%s" % project)
    run("mkdir -p /root/ci/slave/projects/%s" % project)

    put('/tmp/%s.tar.gz' % project, '/tmp/%s.tar.gz' % project)

    with cd('/root/ci/slave/projects/%s' % project):
        run("tar -xf /tmp/%s.tar.gz" % project)


@hosts(slaves)
def omnibus_build():
    with cd('/root/ci/slave/projects/scalarizr-omnibus'):
        run("bundle install --binstubs")
        run("bin/omnibus build project scalarizr")


@hosts(slaves)
def build_source(project='scalarizr'):
    git_export(project)
    with cd('/root/ci/slave/projects/%s' % project):
        run("python setup.py sdist")
    get('/root/ci/slave/projects/scalarizr/dist/%s-*.tar.gz' % project, '/root/ci/master/artifacts')
