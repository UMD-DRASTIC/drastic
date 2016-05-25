"""SearchIndex Model

As Cassandra doesn't provide %LIKE% style queries we are constrained to
only having direct matches and manually checking across each specific
field.  This isn't ideal.

Until such a time as we have a better solution to searching, this model
provides a simple index (and very, very simple retrieval algorithm) for
matching words with resources and collections.  It does *not* search the
data itself.

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
import logging

from indigo.util import default_uuid

class SearchIndex(Model):
    """SearchIndex Model"""
    term = columns.Text(required=True, primary_key=True)
    term_type = columns.Text(required=True, primary_key=True)
    object_path = columns.Text(required=True, primary_key=True)
    object_type = columns.Text(required=True)
    uuid = columns.Text(default=default_uuid)

    @classmethod
    def create(cls, **kwargs):
        """Create a new indexed term"""
        from indigo.models import IDSearch
        idx = super(SearchIndex, cls).create(**kwargs)

        # Create a row in the ID search table
        idx = IDSearch.create(object_path=idx.object_path,
                              term=idx.term,
                              term_type=idx.term_type)
        return idx
    
    @classmethod
    def find(cls, termstrings, user):
        from indigo.models.collection import Collection
        from indigo.models.resource import Resource

        def get_object(obj, user):
            """Return the object corresponding to the SearchIndex object"""
            if obj.object_type == 'Collection':
                result_obj = Collection.find(obj.object_path)
                if not result_obj or not result_obj.user_can(user, "read"):
                    return None
                result_obj = result_obj.to_dict(user)
                result_obj['result_type'] = 'Collection'
                return result_obj
            elif obj.object_type == 'Resource':
                result_obj = Resource.find(obj.object_path)
                # Check the resource's collection for read permission
                if not result_obj or not result_obj.user_can(user, "read"):
                    return None
                result_obj = result_obj.to_dict(user)
                result_obj['result_type'] = 'Resource'
                return result_obj
            return None

        result_objects = []
        for t in termstrings:
            if cls.is_stop_word(t):
                continue
            result_objects.extend(cls.objects.filter(term=t).all())

        results = []
        for result in result_objects:
            try:
                results.append(get_object(result, user))
            except AttributeError:
                logging.warning(u"Problem with SearchIndex('{}','{}','{}','{}')".format(
                                result.uuid,
                                result.term,
                                result.object_type,
                                result.uuid))
        results = [x for x in results if x]

        # Do some sane ordering here to group together by ID and
        # order by frequency. Add the hit_count to the object dictionary
        # and then we can order on that
        keys = set(r['id'] for r in results)

        result_list = []
        for k in keys:
            # get each element with this key, count them, store the hit
            # count and only add one to results
            matches = [x for x in results if x['id'] == k]
            match = matches[0]
            match['hit_count'] = len(matches)
            result_list.append(match)

        return sorted(result_list,
                      key=lambda res: res.get('hit_count', 0),
                      reverse=True)

    @classmethod
    def is_stop_word(cls, term):
        """Check if a term is a stop word"""
        return term in ["a", "an", "and",
                        "the", "of", "is",
                        "in", "it", "or",
                        "to"]

    @classmethod
    def reset(cls, object_path):
        """Delete objects from the SearchIndex"""
        from indigo.models import IDSearch
        rows = IDSearch.find(object_path)
        for id_obj in rows:
            obj = cls.objects.filter(term=id_obj.term,
                                     term_type=id_obj.term_type,
                                     object_path=id_obj.object_path).first()
            if obj:
                obj.delete()
            id_obj.delete()

    @classmethod
    def index(cls, object, fields=['name']):
        """Index"""
        result_count = 0

        def clean(t):
            """Clean a term"""
            if t:
                return t.lower().replace('.', ' ').replace('_', ' ').split(' ')
            else:
                return []

        def clean_full(t):
            """Clean a term but keep all chars"""
            if t:
                return t.lower()
            else:
                return ""

        terms = []
        if 'metadata' in fields:
            metadata = object.get_cdmi_metadata()
            # Metadata are stored as json string, get_metadata() returns it as
            # a Python dictionary
            for k, v in metadata.iteritems():
                # A value can be a string or a list of string
                if isinstance(v, list):
                    for vv in v:
                        terms.extend([('metadata', el) for el in clean(vv.strip())])
                else:
                    terms.extend([('metadata', el) for el in clean(v.strip())])
            fields.remove('metadata')
        for f in fields:
            attr = getattr(object, f)
            if isinstance(attr, dict):
                for k, v in attr.iteritems():
                    terms.extend([(f, el) for el in clean(v.strip())])
                    terms.append((f, clean_full(v.strip())))
            else:
                terms.extend([(f, el) for el in clean(attr)])
                terms.append((f, clean_full(attr)))
        

        object_type = object.__class__.__name__
        for term_type, term in terms:
            if cls.is_stop_word(term):
                continue
            if len(term) < 2:
                continue
            SearchIndex.create(term=term,
                                term_type=term_type,
                                object_type=object_type,
                                object_path=object.path)
            result_count += 1
        return result_count

    def __unicode__(self):
        return unicode("".format(self.term, self.object_type))
