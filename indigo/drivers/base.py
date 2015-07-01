"""
Drivers are used to interact with different storage media, based on
the url specified for a resource.  For instance, the cassandra:// url
is handled by the cassandra driver, file:// by the FileSystem driver.
"""

class StorageDriver(object):

    def __init__(self, url=None):
        self.url = url

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.
        """
        pass