"""Indigo drivers.

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
    """Parse a url to get the correct driver

    Given a url this function attempts to create a driver which is initialised
    with the appropriate path from the URL. For example, cassandra://IDSTRING
    will return an instance of CassandraDriver whose url property is set to
    IDSTRING.
    """
    scheme, path = parse_url(url)
    if scheme not in DRIVERS:
        raise NoSuchDriverException(u"{} is an unknown protocol".format(scheme))
    return DRIVERS[scheme](path)


def parse_url(url):
    """Parse a URL in components
    """
    from urlparse import urlparse
    url_tuple = urlparse(url)
    return url_tuple.scheme, u"{}{}".format(url_tuple.netloc, url_tuple.path)
