import unittest

from indigo.drivers import get_driver, NoSuchDriverException
from indigo.models.blob import Blob, BlobPart

from nose.tools import raises

class DriverTest(unittest.TestCase):
    _multiprocess_can_split_ = True

    def test_get_driver_cassandra(self):
        d = get_driver("cassandra://1234567890")
        assert d is not None
        assert d.url == "1234567890"

    def test_get_driver_filesystem(self):
        d = get_driver("file://path/to/content.csv")
        assert d is not None
        assert d.url == "path/to/content.csv"

    @raises(NoSuchDriverException)
    def test_get_driver_fail(self):
        d = get_driver("fake://doesntexist")
        assert d is None

    def test_cassandra_driver(self):
        content = "Testing Cassandra Chunks"
        b = Blob.create(parts=[], size=0, hash="")
        bp = BlobPart.create(content=content, blob_id=b.id)
        b.update(parts=[bp.id])

        d = get_driver("cassandra://{}".format(b.id))
        assert d

        result = ''.join(chunk for chunk in d.chunk_content())
        assert result == content