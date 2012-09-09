
def chkconfig(**long_kwds):
	raise NotImplementedError()


def selinuxenabled():
	raise NotImplementedError()


def getsebool(name):
	raise NotImplementedError()


def setsebool(name, persistent=None):
	raise NotImplementedError()		
