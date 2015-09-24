"""Utilities package

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

import collections
import functools
import uuid
from crcmod.predefined import mkPredefinedCrcFun
import struct
import base64
import os.path
import json

IDENT_PEN = 42223
# CDMI ObjectId Length: 8 bits header + 16bits uuid
IDENT_LEN = 24


def _calculate_CRC16(id_):
    """Calculate and return the CRC-16 for the identifier.

    Calculate the CRC-16 value for the given identifier. Return the CRC-16
    integer value.

    ``id_`` should be a bytearray object, or a bytestring (Python 2 str).

    Some doctests:

    >>> self._calculate_CRC16(bytearray([0, 1, 2, 3, 0, 9, 0, 0, 255]))
    41953

    >>> self._calculate_CRC16(bytearray([0, 1, 2, 3, 0, 9, 0, 0, 255]))
    58273

    """
    # Co-erce to bytearray. If already a bytearray this will create a copy
    # so as to avoid side-effects of manipulation for CRC calculation
    id_ = bytearray(id_)
    # Reset CRC bytes in copy to 0 for calculation
    id_[6] = 0
    id_[7] = 0
    # Need to generate CRC func
    crc16fun = mkPredefinedCrcFun('crc-16')
    crc16 = crc16fun(str(id_))
    # Return a 2 byte string representation of the resulting integer
    # in network byte order (big-endian)
    return crc16


def _get_blankID():
    """Return a blank CDMI compliant ID.

    Return a blank CDMI compliant ID with enterprise number etc.
    pre-initialized.

    Enterprise Number:

        The Enterprise Number field shall be the SNMP enterprise number of
        the offering organization that created the object ID, in network
        byte order. See RFC 2578 and
        http://www.iana.org/assignments/enterprise-numbers.
        0 is a reserved value.

    """
    id_length = IDENT_LEN
# TODO: add exceptions back
#     if id_length < 9 or id_length > 40:
#         raise InvalidOptionConfigException(
#             'identifiers',
#             'length',
#             id_length,
#             "Identifier length must be at least 9 and no more than 40"
#         )
    id_ = bytearray([0] * id_length)
    # Set IANA Private Enterprise Number
    #
    # CDMI Spec: Enterprise Number should be in network byte order
    # (big-endian) for 3 bytes starting at byte 1
    # struct cannot pack an integer into 3 bytes, instead
    # pack the PEN into 2 bytes starting at byte 2
    pen = IDENT_PEN
    struct.pack_into("!H", id_, 2, pen)
    # Set ID length
    #
    # CDMI Spec: Length should be 1 bytes starting at byte 5
    # struct cannot pack an integer into 1 byte, instead
    # pack the PEN into 2 bytes starting at byte 4
    # Byte 4 is reserved (zero) but length will not exceed 256 so will
    # only occupy byte 5 when back in network byte order (big-endian)
    struct.pack_into("!H", id_, 4, id_length)
    return id_


def _insert_CRC16(id_):
    """Calculate and insert the CRC-16 for the identifier.

    Calculate the CRC-16 value for the given identifier, insert it
    into the given identifier and return the resulting identifier.
    """
    crc16 = _calculate_CRC16(id_)
    struct.pack_into("!H", id_, 6, crc16)
    return id_


def default_cdmi_id():
    """Return a new CDMI ID"""
    # Get a blank CDMI ID
    id_ = _get_blankID()
    # Pack after first 8 bytes of identifier in network byte order
    # (big-endian)
    uid = uuid.uuid4()
    struct.pack_into("!16s", id_, 8, uid.bytes)
    # Calculate and insert the CRC-16
    id_ = _insert_CRC16(id_)
    return base64.b16encode(id_)


def default_uuid():
    """Return a new UUID"""
    return unicode(uuid.uuid4())


class memoized(object):
    """"Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if args in self.cache:
            return self.cache[args]
        else:
            value = self.func(*args)
            self.cache[args] = value
            return value

    def __repr__(self):
        '''Return the function's docstring.'''
        return self.func.__doc__

    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)


class IterStreamer(object):
    """
    File-like streaming iterator.
    """
    def __init__(self, generator):
        self.generator = generator
        self.iterator = iter(generator)
        self.leftover = ''

    def __len__(self):
        return self.generator.__len__()

    def __iter__(self):
        return self.iterator

    def next(self):
        print "Asked for next...."
        return self.iterator.next()

    def read(self, size):
        data = self.leftover
        count = len(self.leftover)
        try:
            while count < size:
                chunk = self.next()
                data += chunk
                count += len(chunk)
        except StopIteration, e:
            self.leftover = ''
            return data

        if count > size:
            self.leftover = data[size:]

        return data[:size]


def meta_cassandra_to_cdmi(metadata):
    """Transform a metadata dictionary retrieved from Cassandra to a CDMI
    metadata dictionary"""
    md = {}
    for k, v in metadata.items():
        try:
            # Values are stored as json strings {'json': val}
            val_json = json.loads(v)
            val = val_json.get('json', '')
            # meta with no values are deleted (not easy to delete them with
            # cqlengine)
            if val:
                md[k] = val
        except ValueError:
            # If there's a ValueError when loading json it's probably
            # because it wasn't stored as json
            if v:
                md[k] = v
    return md


def meta_cdmi_to_cassandra(metadata):
    """Transform a metadata dictionary from CDMI request to a metadata
    dictionary that can be stored in a Cassandra Model"""
    d = {}
    for key, value in metadata.iteritems():
        # Don't store metadata without value
        if not value:
            continue
        # Convert str to unicode
        if isinstance(value, str):
            value = unicode(value)
        json_val = {}
        json_val["json"] = value
        d[key] = json.dumps(json_val)
    return d


def metadata_to_list(metadata):
    """Transform a metadata dictionary retrieved from Cassandra to a list of
    tuples. If some metadata are lists they are splitted in several pairs in
    the result list"""
    res = []
    for k, v in metadata.iteritems():
        try:
            val_json = json.loads(v)
            val = val_json.get('json', '')
            # If the value is a list we create several pairs in the result
            if isinstance(val, list):
                for el in val:
                    res.append((k, el))
            else:
                if val:
                    res.append((k, val))
        except ValueError:
            if v:
                res.append((k, v))
    return res


def merge(coll_name, resc_name):
    """Create a full path from a collection name and a resource name"""
    if coll_name == '/':
        # For root we don't add the extra '/'
        return unicode("{}{}".format(coll_name, resc_name))
    else:
        return unicode("{}/{}".format(coll_name, resc_name))


def split(path):
    """Parse a full path and return the collection and the resource name"""
    coll_name = os.path.dirname(path)
    resc_name = os.path.basename(path)
    return (coll_name, resc_name)
