from indigo.drivers.filesystem import FileSystemDriver
from indigo.drivers.cassandra import CassandraDriver
from indigo.drivers.test import TestDriver
from indigo.models.errors import NoSuchDriverException

DRIVERS = {
    "cassandra": CassandraDriver,
    "file": FileSystemDriver,
    "test": TestDriver,
}

def get_driver(url):
    """
    Given a url this function attempts to create a driver which is initialised
    with the appropriate path from the URL. For example, cassandra://IDSTRING
    will return an instance of CassandraDriver whose url property is set to
    IDSTRING.
    """
    scheme, path = parse_url(url)
    if not scheme in DRIVERS:
        raise NoSuchDriverException(u"{} is an unknown protocol".format(scheme))
    return DRIVERS[scheme](path)

def parse_url(url):
    from urlparse import urlparse
    u = urlparse(url)
    return u.scheme, u"{}{}".format(u.netloc, u.path)
