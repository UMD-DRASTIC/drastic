"""
This module is responsible for loading a schema-definition file
which will define, for collections and resources what metadata
is desired.  This will be loaded into any forms with default empty
values.

Metadata description files should look like the following:

{
    "collections": [
        {
            "name": "field-name",
            "required": true,
            "choices": ["a", "b"] # Optional constrained choices
        }
    ],
    "resources" .... as above
}

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
import json
import os

RESOURCE_METADATA = []
COLLECTION_METADATA = []


def get_resource_keys(fileobj=None):
    ensure_metadata(fileobj=fileobj)
    return [r['name'] for r in RESOURCE_METADATA]

def get_collection_keys(fileobj=None):
    ensure_metadata(fileobj=fileobj)
    return [c['name'] for c in COLLECTION_METADATA]

def get_resource_validator(fileobj=None):
    ensure_metadata(fileobj=fileobj)
    return MetadataValidator(RESOURCE_METADATA)

def get_collection_validator(fileobj=None):
    ensure_metadata(fileobj=fileobj)
    return MetadataValidator(COLLECTION_METADATA)

def ensure_metadata(reload=False, fileobj=None):
    """
    If no file-like object is passed to this function, the file
    location will be found via the INDIGO_METADATA var.
    """
    global RESOURCE_METADATA
    global COLLECTION_METADATA

    if not reload and RESOURCE_METADATA and COLLECTION_METADATA:
        return

    try:
        if not fileobj:
            fileobj = open(os.environ.get('INDIGO_METADATA', ''), 'r')
        data = fileobj.read()
        obj = json.loads(data)
        RESOURCE_METADATA = obj['resources']
        COLLECTION_METADATA = obj['collections']
    except Exception, e:
        print "$INDIGO_METADATA is not set"

class MetadataValidator(object):
    """
    When provided with a specific collection of metadata, this
    class can perform validation of the metadata to make sure
    that it is correct.
    """

    def __init__(self, collection, *args, **kwargs):
        self.rules = {}
        for item in collection:
            name = item['name']
            self.rules[name] = item
        super(MetadataValidator, self).__init__(*args, **kwargs)


    def validate(self, input_data):
        """
        Validates the input data dictionary against the collection
        of validation rules provided to the constructor.
        """
        validation_errors = []
        for k, v in input_data.iteritems():
            err = False

            if not k in self.rules:
                continue

            required = self.rules[k].get('required', False)
            if required and not v.strip():
                validation_errors.append(u'{} is a required field'.format(k))
                err = True

            choices = self.rules[k].get('choices', [])
            if not err and choices and not v in choices:
                if v.strip() or required:
                    validation_errors.append(u'{} is not a valid option for the {} field, choices are: {}'.format(v.strip() or '""', k, ','.join(choices)))
                    err = True

        return len(validation_errors) == 0, validation_errors


