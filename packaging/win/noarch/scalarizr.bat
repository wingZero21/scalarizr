@setlocal enabledelayedexpansion && "%~dp0\Python27\python" -x "%~f0" %* & exit /b !ERRORLEVEL!
import sys
try:
	from scalarizr.app import main
except ImportError, e:
	print "error: %s\n\nPlease make sure that scalarizr is properly installed" % (e)
	sys.exit(1)
main()
