#!/usr/bin/python

import sys, os
from optparse import OptionParser
import logging

pwd = os.path.dirname(__file__)
try:
	import scalarizr
except ImportError:
	sys.path.append(os.path.abspath(pwd  + '/../../src'))
	import scalarizr

try:
	import szr_integtest_libs, szr_integtest
except ImportError:
	sys.path.append(pwd + '/libs')
	sys.path.append(pwd + '/testcases')
	import szr_integtest_libs, szr_integtest
	
	
	
parser = OptionParser()
nightly = parser.add_option_group('Create nightly build')
nightly.add_option('--nightly', dest='nightly', action='store_true', help='Create nightly build')

rc = parser.add_option_group('Create release')
rc.add_option('--release', dest='release', action='store_true', help='Create release candidate')
rc.add_option('-t', '--tag', dest='tag', action='store', help='Tag name (ex: 0.5.10)')

parser.parse_args()
vals = parser.values

if not (vals.release or vals.nightly):
	parser.print_help()
	sys.exit(1)
	
if vals.release and not vals.tag:
	print 'tag required'
	sys.exit(1)
	



