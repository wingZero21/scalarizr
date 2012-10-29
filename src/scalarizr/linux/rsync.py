
import os

from scalarizr.linux import pkgmgr


def rsync(src, dst, **long_kwds):
	if not os.path.exists('/usr/bin/rsync'):
		pkgmgr.package_mgr().install('rsync')

	raise NotImplementedError()
