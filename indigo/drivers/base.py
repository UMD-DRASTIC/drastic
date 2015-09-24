"""Indigo Base Driver.

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
