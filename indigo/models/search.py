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
    id = columns.Text(primary_key=True, default=default_uuid)
    term = columns.Text(required=True)
    term_type = columns.Text(required=True)
    object_type = columns.Text(required=True)
    object_id = columns.Text(required=True)

    @classmethod
    def find(cls, termstrings, user):
        """Search for terms in the archive"""
        # termstrings should have been lower cased and cleaned
        from indigo.models.collection import Collection
        from indigo.models.resource import Resource
        from indigo.models.search2 import SearchIndex2

        def get_object(obj, user):
            """Return the object corresponding to the SearchIndex object"""
            if obj.object_type == 'Collection':
                result_obj = Collection.find_by_id(obj.object_id)
                if not result_obj or not result_obj.user_can(user, "read"):
                    return None

                result_obj = result_obj.to_dict(user)
                result_obj['result_type'] = 'Collection'
                return result_obj
            elif obj.object_type == 'Resource':
                result_obj = Resource.find_by_id(obj.object_id)
                # Check the resource's collection for read permission
                if not result_obj or not result_obj.user_can(user, "read"):
                    return None

                result_obj = result_obj.to_dict(user)
                result_obj['result_type'] = 'Resource'
                return result_obj

            return None

        #terms = [t for t in termstrings if not cls.is_stop_word(t)]

        result_objects = []
        for t in termstrings:
            if cls.is_stop_word(t):
                continue
            result_objects.extend(cls.objects.filter(term=t).all())
            result_objects.extend(SearchIndex2.objects.filter(term=t).all())

        results = []
        for result in result_objects:
            try:
                results.append(get_object(result, user))
            except AttributeError:
                logging.warning(u"Problem with SearchIndex('{}','{}','{}','{}')".format(
                                result.id,
                                result.term,
                                result.object_type,
                                result.object_id))
            result.delete()
        #results = filter(lambda x: x, results)
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
    def reset(cls, id):
        """Delete objects from the SearchIndex"""
        # Have to delete one at a time without a partition index.
        for obj in cls.objects.filter(object_id=id).all():
            obj.delete()

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
            metadata = object.get_metadata()
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
                               object_id=object.id)
            result_count += 1
        return result_count


    def __unicode__(self):
        return unicode("".format(self.term, self.object_type))
