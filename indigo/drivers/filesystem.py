
from indigo.drivers.base import StorageDriver

class FileSystemDriver(StorageDriver):

    chunk_size = 1024 * 1024 * 1

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.
        """
        pass