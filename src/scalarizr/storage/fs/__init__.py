

class FileSystem:
	freezable = False
	
	def mkfs(self, device, **options):
		'''
		Create filesystem on given device
		'''
		pass
	
	def resize(self, device, size=None, **options):
		'''
		Resize filesystem on given device to given size (default: to the size of partition)   
		'''
		pass
	
	def _set_label(self, device, label):
		pass
	
	def _get_label(self, device):
		pass
	
	label = property(_get_label, _set_label)
	'''
	Volume label
	'''
