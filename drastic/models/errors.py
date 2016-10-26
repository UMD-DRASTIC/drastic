"""Drastic Exceptions

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


class BaseError(Exception):
    """Drastic Base Exception."""
    pass


class NoReadAccessError(BaseError):
    """ACL Exception for read access."""
    pass


class NoWriteAccessError(BaseError):
    """ACL Exception for write access."""
    pass


class NoSuchDriverError(BaseError):
    """Drastic Base Exception."""
    pass


class ModelError(BaseError):
    """Base Class for storage Exceptions

    Abstract Base Class from which more specific Exceptions are derived.
    """

    def __init__(self, obj_str):
        self.obj_str = obj_str


class ResourceConflictError(ModelError):
    """Resource already exists Exception"""

    def __str__(self):
        return "Resource already exists at '{}'".format(self.obj_str)


class NoSuchResourceError(ModelError):
    """No such data object Exception"""

    def __str__(self):
        return "Resource '{}' does not exist".format(self.obj_str)


class CollectionConflictError(ModelError):
    """Container already exists Exception"""

    def __str__(self):
        return "Container already exists at '{}'".format(self.obj_str)


class NoSuchCollectionError(ModelError):
    """No such container Exception """

    def __str__(self):
        return "Container '{}' does not exist".format(self.obj_str)


class GroupConflictError(ModelError):
    """Group already exists Exception"""

    def __str__(self):
        return "Group '{}' already exists".format(self.obj_str)


class NodeConflictError(ModelError):
    """Node address already used"""

    def __str__(self):
        return "Address '{}' already in use".format(self.obj_str)


class UserConflictError(ModelError):
    """USername already used"""

    def __str__(self):
        return "Username '{}' already in use".format(self.obj_str)


class UndiagnosedModelError(ModelError):
    """Undiagnosed Exception wrapper

    A catchall Exception raised due to a situation that has not yet been
    diagnosed and dealt with specifically. This wraps the Exception raised
    from the underlying storage implementation.
    """

    def __init__(self, path, exc):
        self.path = path
        # The Exception raised by the underlying storage implementation
        self.exc = exc

    def __str__(self):
        return ("Operation on {0} caused an exception:\n{1}"
                "".format(self.path, self.exc)
                )


class UniqueError(BaseError):
    """Uniqueness error"""
    pass
