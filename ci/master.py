from buildbot.schedulers.basic import AnyBranchScheduler
from buildbot.schedulers.triggerable import Triggerable
from buildbot.changes.filter import ChangeFilter
from buildbot.process.factory import BuildFactory
from buildbot.steps.master import MasterShellCommand
from buildscripts import steps as buildsteps


project = __opts__['project']


c['schedulers'].append(AnyBranchScheduler(
    name=project,
    change_filter=ChangeFilter(project=project, category='default'),
    builderNames=['{0} source'.format(project)]
))


c["schedulers"].append(Triggerable(
    name="{0} packaging".format(project),
    builderNames=["deb_packaging", "rpm_packaging"]
))


def push_to_github(__opts__):
    return [
        MasterShellCommand(
            command="""
            cd sandboxes/{0}/public 
            git pull --rebase private master
            git push origin master""".format(project),
            description='Pushing commit to GitHub',
            descriptionDone='Push commit to GitHub (trunk)'),
    ]


c['builders'].append(dict(
    name='{0} source'.format(project),
    slavenames=['ubuntu1204'],
    factory=BuildFactory(steps=
        #buildsteps.svn(__opts__) +
        #buildsteps.bump_version(__opts__, setter='cat > src/scalarizr/version') +
        buildsteps.git(__opts__) +
        buildsteps.bump_version_for_git(__opts__, setter='cat > src/scalarizr/version') +
        buildsteps.source_dist(__opts__) +
        buildsteps.trigger_packaging(__opts__) + 
        buildsteps.to_repo(__opts__, types=["deb", "rpm"]) +
        push_to_github(__opts__)
    )
))