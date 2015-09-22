from indigo.models.resource import Resource
from indigo.models.collection import Collection


def path_exists(path):
    return is_resource(path) or is_collection(path)


def is_resource(path):
    return Resource.find_by_path(path) is not None


def is_collection(path):
    return Collection.find_by_path(path) is not None
