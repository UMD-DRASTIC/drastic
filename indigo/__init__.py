import os
import importlib

from indigo.util import memoized

@memoized
def get_config(module_name=None):
    """
        Retrieves the settings from a python file which is
        either provided as a module:path directly, or one
        configured in the INDIGO_CONFIG environment
        variable.
    """
    if not module_name:
        module_name = os.environ.get("INDIGO_CONFIG", "settings")
    if not module_name:
        raise Exception("Unable to locate configuration module")

    settings = importlib.import_module(module_name)
    config = {}
    for k, v in settings.__dict__.iteritems():
        if k.startswith('_'):
            continue
        config[k] = v

    return config

