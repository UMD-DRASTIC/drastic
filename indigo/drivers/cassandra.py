
from indigo.drivers.base import StorageDriver
from indigo.models.blob import Blob, BlobPart

class CassandraDriver(StorageDriver):

    def __init__(self, url=None):
        super(CassandraDriver, self).__init__(url)
        self.blob = Blob.find(self.url) if self.url else None

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.
        """
        for idstring in self.blob.parts:
            bp = BlobPart.find(idstring)
            yield bp.content