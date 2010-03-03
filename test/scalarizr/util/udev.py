'''
Created on Mar 2, 2010

@author: marat
'''

import os

f = open("udev-events.log", "a+")
f.write(str(os.environ))
f.write("\n\n")
f.close()