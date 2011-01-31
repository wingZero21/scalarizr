#!/usr/bin/python
from optparse import OptionParser
from scalarizr.util.software import whereis
import subprocess
import os


platforms	= ['ec2', 'rs', 'euca']
dists		= ['centos5', 'ubuntu804', 'ubuntu104']

pwd			= os.path.dirname(__file__)
nosetests	= whereis('nosetests')
opts		= OptionParser()

opts.add_option('-v', '--verbose', action='store_true', default=False, help='Verbose output')
opts.add_option('-p', '--platform', dest='platform', action='append', help='Platform name')
opts.add_option('-t', '--tags', dest='tags', action='append', help='Test tags')
opts.add_option('-d', '--dist', dest='dist', action='append', help='Name of linux distro to test on')
opts.add_option('-n', '--nose-options', dest='noseopts', default=None, help='Command line options for nosetests.')


opts.parse_args()
vals = opts.values

if not vals.platform or not ('all' in vals.platform or set(vals.platform) <= set(platforms)):
	print 'error: Wrong or empty platform name.'
	opts.print_help()
	sys.exit(1)
	
if 'all' in vals.platform: vals.platform = platforms 
	
if not vals.dist or not ('all' in vals.dist or set(vals.dist) <= set(dists)):
	print 'error: Wrong or empty distr name.'
	opts.print_help()
	sys.exit(1)

if 'all' in vals.dist: vals.dist = dists

cmd = [nosetests]
cmd.extend(vals.noseopts.split())
env={}

for platform in vals.platform:
	env.update({'platform': platform})
	for dist in vals.dist:
		env.update({'dist': dist})
		p = subprocess.Popen(cmd, env=env, shell=True)
		out, err = p.communicate()
		print '>>>>>>>>>>>>> Platform: %s, Dist: %s' % (platform, dist)
		print out, err