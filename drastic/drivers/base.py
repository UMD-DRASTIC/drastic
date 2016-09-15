"""Drastic Base Driver.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


class StorageDriver(object):
    """Base Class to describe a driver

    Drivers are used to interact with different storage media, based on
    the url specified for a resource.  For instance, the cassandra:// url
    is handled by the cassandra driver, file:// by the FileSystem driver.
    """

    def __init__(self, url=None):
        self.url = url

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.
        """
        pass
