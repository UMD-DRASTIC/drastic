import requests
from indigo.drivers.base import StorageDriver

class TestDriver(StorageDriver):

    chunk_size = 1024 * 1024 * 1

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.

        The data for this file is most likely to come from
        an agent that is configured to serve the data - this
        comes from the IP address specified in the URL.

        test://path/to/file
        """
        with open(self.url, 'r') as f:
            while True:
                data = f.read(TestDriver.chunk_size)
                if not data:
                    break
                yield data
