"""Indigo Filesystem Driver.

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

import requests

from indigo.drivers.base import StorageDriver


class FileSystemDriver(StorageDriver):
    """Filesystem Driver, used to yield content stored in a filesystem,
    through an Indigo agent web service"""

    chunk_size = 1024 * 1024 * 1

    def chunk_content(self):
        """
        Yields the content for the driver's URL, if any
        a chunk at a time.  The value yielded is the size of
        the chunk and the content chunk itself.

        The data for this file is most likely to come from
        an agent that is configured to serve the data - this
        comes from the IP address specified in the URL.
        """
        parts = self.url.split('/')
        ip = parts[0]

        source = "http://{}:9000/get/{}".format(ip, '/'.join(parts[1:]))

        r = requests.get(source, stream=True)
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                yield chunk
