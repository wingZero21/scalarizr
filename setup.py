import os
import sys

def isbad(name):
    """ Whether name should not be installed """
    return (name.startswith('.') or
            name.startswith('#') or
            name.endswith('.pickle') or
            name == 'CVS')

def isgood(name):
    """ Whether name should be installed """
    return not isbad(name)

def make_data_files(prefix, dir):
    """ Create distutils data_files structure from dir

    distutil will copy all file rooted under dir into prefix, excluding
    dir itself, just like 'ditto src dst' works, and unlike 'cp -r src
    dst, which copy src into dst'.

    Typical usage:
        # install the contents of 'wiki' under sys.prefix+'share/moin'
        data_files = makeDataFiles('share/moin', 'wiki')

    For this directory structure:
        root
            file1
            file2
            dir
                file
                subdir
                    file

    makeDataFiles('prefix', 'root')  will create this distutil data_files structure:
        [('prefix', ['file1', 'file2']),
         ('prefix/dir', ['file']),
         ('prefix/dir/subdir', ['file'])]

    """
    # Strip 'dir/' from of path before joining with prefix
    dir = dir.rstrip('/')
    strip = len(dir) + 1
    found = []
    os.path.walk(dir, visit, (prefix, strip, found))
    return found

def visit((prefix, strip, found), dirname, names):
    """ Visit directory, create distutil tuple

    Add distutil tuple for each directory using this format:
        (destination, [dirname/file1, dirname/file2, ...])

    distutil will copy later file1, file2, ... info destination.
    """
    files = []
    # Iterate over a copy of names, modify names
    for name in names[:]:
        path = os.path.join(dirname, name)
        # Ignore directories -  we will visit later
        if os.path.isdir(path):
            # Remove directories we don't want to visit later
            if isbad(name):
                names.remove(name)
            continue
        elif isgood(name):
            files.append(path)
    destination = os.path.join(prefix, dirname[strip:])
    found.append((destination, files))

data_files = make_data_files("/etc/scalarizr", "etc")
data_files.append(("/", ["dist/rpm/scalarizr.init.d"]))
data_files.append(("/", ["dist/rpm/scalarizr.sysconfig"]))


from distutils.command.build_scripts import build_scripts
from distutils.command.install_scripts import install_scripts

class my_install_scripts(install_scripts):
	def run(self):
		install_scripts.run(self)
		print self.__dict__
		"""
		build_scripts.copy_scripts(self)
		
		print self.__dict__
		
		# Copy system scripts
		scripts = ["udev", "reboot", "halt"]
		for script_name in scripts:
			script_vars = {
				"python" : os.path.normpath(self.executable),
				"package" : "scalarizr.scripts." + script_name
			}
			pass
		"""
	




cfg = dict(
	name="scalarizr",
	version="0.5",	 
	description="Scalarizr converts any server to Scalr-manageable node",
	author="Scalr Inc.",
	package_dir={"" : "src"},
	cmdclass={"install_scripts" : my_install_scripts},
	packages=[
		"scalarizr", 
		"scalarizr.handlers", 
		"scalarizr.handlers.ec2",
		"scalarizr.messaging",
		"scalarizr.messaging.p2p",
		"scalarizr.platform",
		"scalarizr.scripts",
		"scalarizr.util" 
	],
	scripts=['bin/scalarizr'],
	data_files=make_data_files("/etc/scalarizr", "etc")
)


from distutils.core import setup
setup(**cfg)


