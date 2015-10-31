"""ID Index Model

ID index is our own index for id of collections/resources.

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


from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

from indigo.util import default_uuid


class IDIndex(Model):
    """Group Model
    
    - id: The uuid of the element we want to index
    - cls a python class name we can use to get associated object
    - key: a string we can send to the object class as a primary key
    """
    id = columns.Text(primary_key=True, default=default_uuid)
    classname = columns.Text(required=True, index=True)
    key = columns.Text(required=True)
    
    @classmethod
    def find(cls, uuid):
        """Find an object by its uuid"""
        return cls.objects.filter(id=uuid).first()
