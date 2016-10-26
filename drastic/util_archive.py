"""Utilities package

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


from drastic.models import (
    Collection,
    Resource,
)


def is_collection(path):
    """Check if the collection exists"""
    return Collection.find(path) is not None


def is_resource(path):
    """Check if the resource exists"""
    return Resource.find(path) is not None


def path_exists(path):
    """Check if the path is already in use"""
    return is_resource(path) or is_collection(path)

