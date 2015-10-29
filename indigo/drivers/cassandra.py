"""Indigo Cassandra Driver.

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

from cStringIO import StringIO
import logging
import zipfile

from indigo.drivers.base import StorageDriver
from indigo.models.blob import (
    Blob,
    BlobPart
)


class CassandraDriver(StorageDriver):
    """Cassandra Driver, used to yield content stored in a Cassandra database
    """

    def __init__(self, url=None):
        super(CassandraDriver, self).__init__(url)
        self.blob = Blob.find(self.url) if self.url else None

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.
        """
        for blob_id in self.blob.parts:
            bp = BlobPart.find(blob_id)
            if bp.compressed:
                data = StringIO(bp.content)
                z = zipfile.ZipFile(data, 'r')
                content = z.read("data")
                data.close()
                z.close()
                yield content
            else:
                yield bp.content

    def delete_blob(self):
        logging.debug('Deleting blob "{0}"'.format(self.blob))

        for blob_id in self.blob.parts:
            bp = BlobPart.find(blob_id)
            logging.debug('Deleting blobpart "{0}"'.format(bp))
            bp.delete()

        self.blob.delete()
