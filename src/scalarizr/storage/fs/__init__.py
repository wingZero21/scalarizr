from scalarizr.util import system
import os

MKFS_PATH		= "mkfs"
MOUNT_PATH		= "/bin/mount"

class FileSystem:
	freezable = False
	
	def mkfs(self, device, **options):
		
		if not hasattr(self, '_fsname'):
			raise Exception("Use specialized filesystem class instead of 'FileSystem'")
		
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		cmd = '%s -t %s %s' % (MKFS_PATH, self._fsname, device)
		error = "Error occured during filesystem creation on device '%s'" % device
		self._system(cmd, error)
	
	def resize(self, device, size=None, **options):
		'''
		Resize filesystem on given device to given size (default: to the size of partition)   
		'''
		pass
	
	def set_label(self, device, label):
		pass
	
	def get_label(self, device):
		pass
	
	def _system(self, cmd, error, shell=False):
		out,rcode = system(cmd + ' 2>&1', shell)[0::2]
		if rcode:
			raise Exception(error+"\n" + "Return code: %s. Error: %s" % (rcode, out))
		return out
	
#	label = property(_get_label, _set_label)
	'''
	Volume label
	'''
