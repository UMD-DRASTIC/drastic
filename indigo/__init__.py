"""Indigo Base package

Copyright 2015 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

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
    for key, val in settings.__dict__.iteritems():
        if key.startswith('_'):
            continue
        config[key] = val

    return config
