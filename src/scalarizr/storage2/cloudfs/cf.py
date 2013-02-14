
from scalarizr.storage2 import cloudfs
from scalarizr.storage2.cloudfs import swift


cloudfs.cloudfs_types["cf"] = swift.SwiftFileSystem