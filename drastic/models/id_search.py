"""ID Search Model

ID search is our own index for id of search terms.

"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


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

