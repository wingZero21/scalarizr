from __future__ import with_statement
'''
Created on Mar 1, 2010

@author: marat
'''

from scalarizr.handlers.block_device import BlockDeviceHandler
from scalarizr.platform.ec2 import ebstool

from scalarizr import linux


def get_handlers ():
    if linux.os.windows_family:
        return []
    else:
        return [EbsHandler()]

class EbsHandler(BlockDeviceHandler):

    def __init__(self):
        BlockDeviceHandler.__init__(self, 'ebs')

    def get_devname(self, devname):
        return ebstool.get_ebs_devname(devname)
