import os
import sys
__version__ = open(os.path.join(os.path.dirname(__file__), 'version')).read().strip()


'''
class ScalarizrModule(object):
    def __getattr__(self, name):
        if name in globals():
            return globals()[name]
        elif name == 'WindowsService':
            import scalarizr.app
            return scalarizr.app.WindowsService
        else:
            msg = "'{0}' has no attribute '{1}'".format(__name__, name)
            raise AttributeError(msg)


sys.modules[__name__] = ScalarizrModule()
'''