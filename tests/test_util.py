"""unittest class for utility package 

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import unittest

from common import *
from drastic.util_archive import (
    path_exists,
    is_resource,
    is_collection,
)

class TestUtil(DrasticTestCase):

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