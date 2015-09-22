

class BaseException(Exception):
    """Indigo Base Exception."""
    pass


class NoSuchDriverException(BaseException):
    pass


class UniqueException(BaseException):
    pass


class NoSuchCollection(BaseException):
    pass


class StorageException(BaseException):
    """Base Class for storage Exceptions.
 
    Abstract Base Class from which more specific Exceptions are derived.
    """

    def __init__(self, path):
        self.path = path


class NoSuchObjectException(StorageException):
    """No such object Exception.
 
    An Exception raised due to request to carry out an action on an object
    that does not exist.
    """

    def __str__(self):
        return "Object {0} does not exist".format(self.path)


class NoSuchContainerException(NoSuchObjectException):
    """No such container Exception.

    An Exception raised due to request to carry out an action on a path
    for which the the container does not exist.
    """

    def __str__(self):
        return "Container {0} does not exist".format(self.path)


class NotAContainerException(StorageException):
    """Resource is not a container Exception.
 
    An Exception raised due to request to carry out a container specific
    action on a path that is not a container.
    """
 
    def __str__(self):
        return "{0} is not a container".format(self.path)
 
 
class NoSuchDataObjectException(NoSuchObjectException):
    """No such data object Exception.
 
    An Exception raised due to request to carry out an action on a data object
    which does not exist.
    """
 
    def __str__(self):
        return "Data object {0} does not exist".format(self.path)
 
 
class NotADataObjectException(StorageException):
    """Resource is not a data object Exception.
 
    An Exception raised due to request to carry out a data object specific
    action on a path that is not a data object.
    """
 
    def __str__(self):
        return "{0} is not a data object".format(self.path)
 
 
class ContainerAlreadyExistsException(StorageException):
    """Container already exists Exception.
 
    An Exception raised due to request to create a resource on a path that
    is already present as a container on the storage resource.
    """
 
    def __str__(self):
        return "Container already exists at {0}".format(self.path)
 
 
class DataObjectAlreadyExistsException(StorageException):
    """Resource already exists Exception.
 
    An Exception raised due to request to create a resource on a path that
    is already present as a data object on the storage resource.
    """
# 
#     def __str__(self):
#         return "Data object already exists at {0}".format(self.path)
# 
# 
# class UndiagnosedStorageException(StorageException):
#     """Undiagnosed Exception wrapper.
# 
#     A catchall Exception raised due to a situation that has not yet been
#     diagnosed and dealt with specifically. This wraps the Exception raised
#     from the underlying storage implementation.
#     """
# 
#     def __init__(self, path, exc):
#         self.path = path
#         # The Exception raised by the underlying storage implementation
#         self.exc = exc
# 
#     def __str__(self):
#         return ("Operation on {0} caused an exception:\n{1}"
#                 "".format(self.path, self.exc)
#                 )


# class ConfigException(BaseException):
#     """Indigo configuration Exception.
# 
#     An Exception raised due to missing or invalid configuration
#     value.
#     """
#     pass
# 
# 
# class NoSectionConfigException(ConfigException):
#     """Indigo configuration missing option exception.
# 
#     An Exception raised due to a missing configuration section.
#     """
# 
#     def __init__(self, section, info=""):
#         self.section = section
#         self.info = info
# 
#     def __str__(self):
#         message = ("Missing configuration section '{section}'."
#                    "".format(**self.__dict__)
#                    )
#         if self.info:
#             message += "\n" + self.info
#         return message
# 
# 
# class NoOptionConfigException(ConfigException):
#     """Indigo configuration missing option exception.
# 
#     An Exception raised due to a missing configuration option.
#     """
# 
#     def __init__(self, section, option, info=""):
#         self.section = section
#         self.option = option
#         self.info = info
# 
#     def __str__(self):
#         message = ("Missing configuration option '{option}' in section "
#                    "'{section}'.".format(**self.__dict__)
#                    )
#         if self.info:
#             message += "\n" + self.info
#         return message
# 
# 
# class InvalidOptionConfigException(ConfigException):
#     """Indigo configuration invalid option exception.
# 
#     An Exception raised due to an incorrect or inappropriate configuration
#     value.
#     """
# 
#     def __init__(self, section, option, value, info=""):
#         self.section = section
#         self.option = option
#         self.value = value
#         self.info = info
# 
#     def __str__(self):
#         message = ("Invalid configuration value '{value}' for option "
#                    "'{option}' in section '{section}'."
#                    "".format(**self.__dict__)
#                    )
#         if self.info:
#             message += "\n" + self.info
#         return message


class NoReadAccess(BaseException):
    pass
 
 
class NoWriteAccess(BaseException):
    pass
 
# 
# class ResourceAlreadyExists(BaseException):
#     pass
# 
# 
# class ResourceDoesntExist(BaseException):
#     pass
# 
 
class ResourcePolicyConflict(BaseException):
    pass
# 
# 
# class RoleDoesntExist(BaseException):
#     pass
# 
# 
# class SiteAlreadyExists(BaseException):
#     pass
# 
# 
# class SiteDoesntExist(BaseException):
#     pass
