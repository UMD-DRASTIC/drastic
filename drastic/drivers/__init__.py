"""Drastic drivers.
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


from drastic.drivers.filesystem import FileSystemDriver
from drastic.drivers.cassandra import CassandraDriver
from drastic.drivers.test import TestDriver
from drastic.models.errors import NoSuchDriverError

DRIVERS = {
    "cassandra": CassandraDriver,
    "file": FileSystemDriver,
    "test": TestDriver,
}


def get_driver(url):
    """Parse a url to get the correct driver

    Given a url this function attempts to create a driver which is initialised
    with the appropriate path from the URL. For example, cassandra://IDSTRING
    will return an instance of CassandraDriver whose url property is set to
    IDSTRING.
    """
    scheme, path = parse_url(url)
    if scheme not in DRIVERS:
        raise NoSuchDriverError(u"{} is an unknown protocol".format(scheme))
    return DRIVERS[scheme](path)


def parse_url(url):
    """Parse a URL in components
    """
    from urlparse import urlparse
    url_tuple = urlparse(url)
    return url_tuple.scheme, u"{}{}".format(url_tuple.netloc, url_tuple.path)
