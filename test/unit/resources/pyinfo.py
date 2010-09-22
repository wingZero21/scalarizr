#!/usr/bin/python
# Intentionally not using /usr/bin/env python
# Why: /usr/local/bin is not passed to CGI scripts by a number of OS
# thus env would be unable to find python.
"""
$URL$
$Id$

pyinfo - A quick look at your Python environment.
"""
import os
import pkgutil
import pprint
import sys
from cgi import escape

def dl(tuples):
    output = u''
    output += '<dl>\n'
    for title, description in tuples:
        if title:
            output += '  <dt>%s</dt>\n' % escape(title)
        if description:
            output += '  <dt>%s</dt>\n' % escape(description)
    output += '</dl>\n'
    return output

def group(seq):
    """(seq:(item, category)) -> {category:items}

    Groups items by supplied category, e.g.:
        group((e, e.tags[0]) for e in journal.get_recent_entries(100))

    Lifted from http://aspn.activestate.com/ASPN/Coo.../Recipe/498223
    """
    result = {}
    for item, category in seq:
        result.setdefault(category, []).append(item)
    return result

def get_packages():
    return set([modname for importer, modname, ispkg in
                   pkgutil.walk_packages(onerror=lambda x:x)
                   if ispkg and '.' not in modname])

def format_packages():
    packages = group((pkg, pkg[0].lower()) for pkg in get_packages())
    # convert ('a',['apackage','anotherapackage]) into ('a', 'apackage, anotherapackage')
    packages = [(letter, ', '.join(pkgs)) for letter, pkgs in packages.items()]
    return '<h2>Installed Packages</h2>\n%s' % dl(sorted(packages))

def format_environ(environ):
    return '<h2>Environment</h2>\n%s' % dl(sorted(environ.items()))

def format_python_path():
    # differentiate between eggs and regular paths
    eggs = [p for p in sys.path if p.endswith('.egg')]
    paths = [p for p in sys.path if p not in eggs]
    return dl([('Paths', ',\n'.join(paths)),
               ('Eggs', ',\n'.join(eggs)),
              ])

def format_version():
    version, platform = sys.version.split('\n')
    sysname, nodename, release, osversion, machine = os.uname()
    return '<h2>Version</h2>\n%s' % dl([
        ('Python Version', version),
        ('Build Platform', platform),
        ('OS', sysname),
        ('OS Version', osversion),
        ('Machine Type', machine),])

def format():
    output = u''
    output += '<h1>Python Info</h1>\n'
    output += format_version()
    output += format_python_path()
    output += format_environ(os.environ)
    output += format_packages()
    return output

def page(html):
    print "Content-type: text/html"
    print
    print '<html>\n<head><title>%s Python configuration</title></head>' % os.uname()[1]
    print '<body>\n%s</body>\n</html>' % html

if __name__ == '__main__':
    page(format())
