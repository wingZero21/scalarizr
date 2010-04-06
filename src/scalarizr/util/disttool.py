'''
@author: Dmytro Korsakov
'''
import platform


_uname = None
_linux_dist = None

_is_linux = _is_win = _is_sun = False
_is_debian_based = _is_redhat_based = False
_is_debian = _is_ubuntu = False
_is_rhel = _is_centos = _is_fedora = False

_debian_based_dists = ['debian', 'ubuntu']
_redhat_based_dists = ['centos', 'rhel', 'redhat', 'fedora']


_uname = platform.uname()
os = _uname[0].lower()
_is_linux = os == "linux"
_is_win = os == "windows"
_is_sun = os == "sunos"
			
if _is_linux:
	_linux_dist = platform.linux_distribution() \
		if hasattr(platform, "linux_distribution") \
		else platform.dist()
	dist_name = _linux_dist[0].lower()
	_is_redhat_based = dist_name in _redhat_based_dists
	_is_debian_based = dist_name in _debian_based_dists
	_is_debian = dist_name == "debian"
	_is_ubuntu = dist_name == "ubuntu"
	_is_fedora = dist_name == "fedora"
	_is_centos = dist_name == "centos"
	_is_rhel = dist_name in ["rhel", "redhat"]
			

def is_linux(): return _is_linux
def is_win(): return _is_win
def is_sun(): return _is_sun

def is_debian_based(): return _is_debian_based
def is_redhat_based(): return _is_redhat_based

def is_fedora(): return _is_fedora
def is_centos(): return _is_centos
def is_rhel(): return _is_rhel
def is_ubuntu(): return _is_ubuntu
def is_debian(): return _is_debian

def uname(): return _uname
def linux_dist(): return _linux_dist