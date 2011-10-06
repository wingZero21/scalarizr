'''
Created on Sep 9, 2011

@author: marat
'''

import os
import time

from scalarizr.bus import bus
from scalarizr.handlers import HandlerError
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.util import fstool, disttool
from scalarizr.platform.cloudstack import voltool

LOG = rebundle_hdlr.LOG

def get_handlers():
	return [CloudStackRebundleHandler()]


class CloudStackRebundleHandler(rebundle_hdlr.RebundleHandler):
	IMAGE_MPOINT = '/mnt/img-mnt'
	
	def detect_os_type_id(self, conn):
		default_desc = 'Other Ubuntu (%d-bit)' % disttool.arch_bits()
		desc = '%s %s (%d-bit)' % (disttool.linux_dist()[0], 
								disttool.linux_dist()[1], 
								disttool.arch_bits())
		default = 0
		for ostype in conn.listOsTypes():
			if ostype.description == default_desc:
				default = ostype.id
			elif ostype.description == desc:
				return ostype.id
		return default
	
	def rebundle(self):
		image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")
		dirty_snap = vol = snap = device = mounted = bundled = None
		
		pl = bus.platform
		conn = pl.new_cloudstack_conn()
		
		try:
			root_vol = conn.listVolumes(virtualMachineId=pl.get_instance_id())[0]
		except IndexError:
			raise HandlerError("Can't find root volume for virtual machine %s" % pl.get_instance_id())
		
		try:
			# Create snapshot
			LOG.info('Creating root volume (id: %s) snapshot...', root_vol.id)
			dirty_snap = voltool.create_snapshot(conn, root_vol.id,  
												wait_completion=True, logger=LOG)
			LOG.info('Snapshot (id: %s) created', dirty_snap.id)
			
			# Created temporary volume to perform cleanups
			LOG.info('Creating volume for image cleanups')
			vol = voltool.create_volume(conn, name=image_name + '-tmp', 
											snap_id=dirty_snap.id, logger=LOG)
			device = voltool.attach_volume(conn, vol.id, pl.get_instance_id(), 
												to_me=True, logger=LOG)[1]
			LOG.info('Volume (id: %s) created', vol.id)
			
			# Mount image
			fstool.mount(device, self.IMAGE_MPOINT)
			mounted = True
				
			self.cleanup_image(self.IMAGE_MPOINT)
				
			LOG.info('Creating snapshot for template creation...')
			snap = voltool.create_snapshot(conn, vol.id, 
										wait_completion=True, logger=LOG)
			LOG.info('Snapshot (id: %s) created', snap.id)
				
			LOG.info('Creating image...')
			image = conn.createTemplate(image_name, image_name, 
							self.detect_os_type_id(conn), 
							snapshotId=snap.id)
			LOG.info('Image (id: %s) created', image.id)
			
			return image.id	
		finally:
			if dirty_snap:
				try:
					conn.deleteSnapshot(dirty_snap.id)
				except:
					pass
			if vol:
				if mounted:
					try:
						fstool.umount(device)
					except:
						pass
				if os.path.exists(self.IMAGE_MPOINT):
					os.removedirs(self.IMAGE_MPOINT)
				try:
					conn.deleteVolume(vol.id)
				except:
					pass
			if snap and not bundled:
				try:
					conn.deleteSnapshot(snap.id)
				except:
					pass
				
				