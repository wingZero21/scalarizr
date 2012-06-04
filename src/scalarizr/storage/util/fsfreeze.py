from scalarizr.storage import StorageError
from scalarizr.util.software import whereis
from scalarizr.util import system2

try:
	FSFREEZE = whereis('fsfreeze')[0]
except:
	raise StorageError('Can not find fsfreeze binary, which is essential for raid snapshots')

def freeze(dir):
	system2((FSFREEZE, '-f', dir))


def unfreeze(dir):
	system2((FSFREEZE, '-u', dir))