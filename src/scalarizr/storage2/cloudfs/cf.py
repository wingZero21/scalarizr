
import urlparse

from scalarizr.storage2 import cloudfs
from scalarizr.storage2.cloudfs import swift


class CloudfilesFileSystem(swift.SwiftFileSystem):

	schema = "cf"
	urlparse.uses_netloc.append(schema)

cloudfs.cloudfs_types["cf"] = CloudfilesFileSystem