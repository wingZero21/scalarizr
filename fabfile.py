from fabric.api import *

#env.hosts = ['ubuntu-1', 'ubuntu-2']

@parallel
def runs_in_parallel():
    return run('uname -a')

@runs_once
def runs_serially():
	result = execute(runs_in_parallel)
	puts(result)

"""
def sdist():
	''' create source distribution '''
	pass

def sdist_upload():
	pass

def test():
	''' run unit tests '''
	pass

def bdist():
	''' '''
	pass

@roles()
def omnibus():
	pass

def omnibus_upload():
	pass
"""
