"""ID Search Model

ID search is our own index for id of search terms.

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



class IDSearch(Model):
    """Reverse Search Model (lookup table for search from object_path)
    """
    object_path = columns.Text(required=True, primary_key=True)
    term = columns.Text(required=True, primary_key=True)
    term_type = columns.Text(required=True, primary_key=True)
    
    @classmethod
    def find(cls, object_path):
        """Find all terms associated to an object id"""
        return cls.objects.filter(object_path=object_path).all()

