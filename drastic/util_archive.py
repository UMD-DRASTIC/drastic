"""Utilities package for the archive
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


from drastic.models.resource import Resource
from drastic.models.collection import Collection


def path_exists(path):
    """Check if the path is already in use"""
    return is_resource(path) or is_collection(path)


def is_resource(path):
    """Check if the resource exists"""
    return Resource.find_by_path(path) is not None


def is_collection(path):
    """Check if the collection exists"""
    return Collection.find_by_path(path) is not None
