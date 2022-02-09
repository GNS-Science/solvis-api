
from flask import g, current_app, _app_ctx_stack
# from .model import SolutionLocationsRadiiDF

import api.datastore.resources

"""
A Flask Extension class so we can make all our datastore resources configurable

simplified from https://flask.palletsprojects.com/en/2.0.x/extensiondev/#the-extension-code

"""

_ds = None

"""

"""
def get_datastore(resources=None):
    global _ds
    print("get_datastore", _ds, resources)
    _ds = _ds or Datastore(resources)
    print("get_get_datastore", _ds)
    return _ds

def get_ds():
    global _ds
    if 'db' not in g:
        g.ds = get_datastore()
    return g.ds

class Datastore(object):
    def __init__(self, resources=None):
        print('__init__ with resources', resources)
        res = resources or api.datastore.resources
        self.configure(res)

    def configure(self, resources):
        self._resources = resources
        # self._resources.solutions = SolutionLocationsRadiiDF


    @property
    def resources(self):
        #ctx = _app_ctx_stack.top
        #if ctx is not None:
        return self._resources