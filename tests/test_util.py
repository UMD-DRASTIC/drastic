"""unittest class for utility package 

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

import unittest

from common import *
from indigo.util_archive import (
    path_exists,
    is_resource,
    is_collection,
)

class TestUtil(IndigoTestCase):

    def test_is_collection(self):
        self.create_collection("/unittest")
        self.assertTrue(is_collection("/unittest"))
        self.delete_collection("/unittest")


    def test_is_resource(self):
        self.create_resource("/unittest.txt")
        self.assertTrue(is_collection("/unittest.txt"))
        self.delete_resource("/unittest.txt")



    def test_path_exists(self):
        self.create_collection("/unittest")
        self.assertTrue(path_exists("/unittest"))
        self.assertFalse(path_exists("/unittest_false"))
        self.delete_collection("/unittest")


def suite():
    import logger
    logger.setLevel(logger.ERROR)
    suite = unittest.TestSuite()
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestUtil))
    return suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(suite())