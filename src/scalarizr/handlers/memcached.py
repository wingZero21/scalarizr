'''
Created on Jul 23, 2010

@author: marat
@author: shaitanich
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler

# Explore memcached for initd

def get_handlers():
	return [MemcachedHandler()]

class MemcachedHandler(Handler):
	def __init__(self):
		pass
	
	def on_init(self):
		bus.on("start", self.on_start)
		bus.on("before_host_up", self.on_before_host_up)

	def on_start(self):
		pass
	
	def on_before_host_up(self):
		# Configure and start memcached
		pass
	
	def on_HostUp(self):
		# Add iptables rule
		pass
	
	def on_HostDown(self):
		# Remove iptables rule
		pass




"""
Config samples:

== Fedora 8 - 12
/etc/sysconfig/memcached

PORT="11211"
USER="memcached"
MAXCONN="1024"
CACHESIZE="64"
OPTIONS=""



== Debian 5
/etc/memcached.conf

# memcached default config file
# 2003 - Jay Bonci <jaybonci@debian.org>
# This configuration file is read by the start-memcached script provided as
# part of the Debian GNU/Linux distribution. 

# Run memcached as a daemon. This command is implied, and is not needed for the
# daemon to run. See the README.Debian that comes with this package for more
# information.
-d

# Log memcached's output to /var/log/memcached
logfile /var/log/memcached.log

# Be verbose
# -v

# Be even more verbose (print client commands as well)
# -vv

# Start with a cap of 64 megs of memory. It's reasonable, and the daemon default
# Note that the daemon will grow to this size, but does not start out holding this much
# memory
-m 64

# Default connection port is 11211
-p 11211 

# Run the daemon as root. The start-memcached will default to running as root if no
# -u command is present in this config file
-u nobody

# Specify which IP address to listen on. The default is to listen on all IP addresses
# This parameter is one of the only security measures that memcached has, so make sure
# it's listening on a firewalled interface.
-l 127.0.0.1

# Limit the number of simultaneous incoming connections. The daemon default is 1024
# -c 1024

# Lock down all paged memory. Consult with the README and homepage before you do this
# -k

# Return error when memory is exhausted (rather than removing items)
# -M

# Maximize core file limit
# -r


"""

