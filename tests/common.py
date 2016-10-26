"""Common class for unittest 

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import unittest

from drastic.models import initialise
from drastic.util import (
    split,
)
from drastic.models import (
    Collection,
    Resource,
)


class DrasticTestCase(unittest.TestCase):


    def setUp(self):
        initialise()


    def create_collection(self, path):
        container, name = split(path)
        Collection.create(name, container)


    def create_resource(self, path):
        container, name = split(path)
        Collection.create(name, container)


    def delete_collection(self, path):
        Collection.delete_all(path)


