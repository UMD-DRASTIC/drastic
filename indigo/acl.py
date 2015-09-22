from collections import defaultdict, OrderedDict

from indigo.models.resource import Resource

# ACE Flags for ACL in CDMI
ACEFLAG_NONE = 0x00000000
ACEFLAG_OBJECT_INHERIT = 0x00000001
ACEFLAG_CONTAINER_INHERIT = 0x00000002
ACEFLAG_NO_PROPAGATE = 0x00000004
ACEFLAG_INHERIT_ONLY = 0x00000008
ACEFLAG_IDENTIFIER_GROUP = 0x00000040
ACEFLAG_INHERITED = 0x00000080

# ACE Mask bits for ACL in CDMI
CDMI_ACE_READ_OBJECT = 0x00000001
CDMI_ACE_LIST_CONTAINER = 0x00000001
CDMI_ACE_WRITE_OBJECT = 0x00000002
CDMI_ACE_ADD_OBJECT = 0x00000002
CDMI_ACE_APPEND_DATA = 0x00000004
CDMI_ACE_ADD_SUBCONTAINER = 0x00000004
CDMI_ACE_READ_METADATA = 0x00000008
CDMI_ACE_WRITE_METADATA = 0x00000010
CDMI_ACE_EXECUTE = 0x00000020
CDMI_ACE_DELETE_OBJECT = 0x00000040
CDMI_ACE_DELETE_SUBCONTAINER = 0x00000040
CDMI_ACE_READ_ATTRIBUTES = 0x00000080
CDMI_ACE_WRITE_ATTRIBUTES = 0x00000100
CDMI_ACE_WRITE_RETENTION = 0x00000200
CDMI_ACE_WRITE_RETENTION_HOLD = 0x00000400
CDMI_ACE_DELETE = 0x00010000
CDMI_ACE_READ_ACL = 0x00020000
CDMI_ACE_WRITE_ACL = 0x00040000
CDMI_ACE_WRITE_OWNER = 0x00080000
CDMI_ACE_SYNCHRONIZE = 0x00100000

# Ace flags table
ACEFLAG_TABLE = [
    (0x00000080, "INHERITED"),
    (0x00000040, "IDENTIFIER_GROUP"),
    (0x00000008, "INHERIT_ONLY"),
    (0x00000004, "NO_PROPAGATE"),
    (0x00000002, "CONTAINER_INHERIT"),
    (0x00000001, "OBJECT_INHERIT"),
    (0x00000000, "NO_FLAGS")
]

ACEMASK_TABLE = [
    (0x00100000, "SYNCHRONIZE", "SYNCHRONIZE"),
    (0x00080000, "WRITE_OWNER", "WRITE_OWNER"),
    (0x00040000, "WRITE_ACL", "WRITE_ACL"),
    (0x00020000, "READ_ACL", "READ_ACL"),
    (0x00010000, "DELETE", "DELETE"),
    (0x00000400, "WRITE_RETENTION_HOLD", "WRITE_RETENTION_HOLD"),
    (0x00000200, "WRITE_RETENTION", "WRITE_RETENTION"),
    (0x00000100, "WRITE_ATTRIBUTES", "WRITE_ATTRIBUTES"),
    (0x00000080, "READ_ATTRIBUTES", "READ_ATTRIBUTES"),
    (0x00000040, "DELETE_OBJECT", "DELETE_SUBCONTAINER"),
    (0x00000020, "EXECUTE", "EXECUTE"),
    (0x00000010, "WRITE_METADATA", "WRITE_METADATA"),
    (0x00000008, "READ_METADATA", "READ_METADATA"),
    (0x00000004, "APPEND_DATA", "ADD_SUBCONTAINER"),
    (0x00000002, "WRITE_OBJECT", "ADD_OBJECT"),
    (0x00000001, "READ_OBJECT", "LIST_CONTAINER")
]

CDMI_ACE_DB_MAPPING_DATA = {
    "none": 0x0,
    "read": CDMI_ACE_READ_OBJECT | CDMI_ACE_READ_METADATA,  # 0x09
    "write": 0x56,   # CDMI_ACE_WRITE_OBJECT | CDMI_ACE_APPEND_DATA |
                     # CDMI_ACE_WRITE_METADATA | CDMI_ACE_DELETE_OBJECT,
    "edit": 0x56,
    "delete": CDMI_ACE_DELETE
}

CDMI_ACE_DB_MAPPING_COLL = {
    "none": 0x0,
    "read": CDMI_ACE_LIST_CONTAINER | CDMI_ACE_READ_METADATA,
    "write": 0x56,  # CDMI_ACE_ADD_OBJECT | CDMI_ACE_ADD_SUBCONTAINER |
                    # CDMI_ACE_WRITE_METADATA | CDMI_ACE_DELETE_SUBCONTAINER,
    "edit": 0x56,
    "delete": CDMI_ACE_DELETE | CDMI_ACE_DELETE_OBJECT | CDMI_ACE_DELETE_SUBCONTAINER
}


def aceflag_num_to_str(num_value):
    """Return the string value for ACE flag value given

    Return the string value for the ACE flag value given. It returns a text
    expression simpler to understand.

    :param num_value: ACE flag numeric value
    :type num_value: integer
    :rtype: string
    """
    res = []
    for idx in xrange(len(ACEFLAG_TABLE)):
        if num_value == 0:
            return ', '.join(res)

        if num_value & ACEFLAG_TABLE[idx][0] == ACEFLAG_TABLE[idx][0]:
            res.append(ACEFLAG_TABLE[idx][1])
            num_value = num_value ^ ACEFLAG_TABLE[idx][0]
    return ', '.join(res)


def acemask_num_to_str(num_value, is_object):
    """Return the string value for ACE mask value given.

    Return the string value for the ACE mask value given. It returns a
    text expression simpler to understand.

    :param num_value: ACE mask numeric value
    :type num_value: integer
    :param is_object: True if the ACE relates to an object, False for a c
    :type is_object: boolean
    :rtype: string
    """
    res = []
    for idx in xrange(len(ACEMASK_TABLE)):
        if num_value == 0:
            return ', '.join(res)

        if num_value & ACEMASK_TABLE[idx][0] == ACEMASK_TABLE[idx][0]:
            if is_object:
                res.append(ACEMASK_TABLE[idx][1])
            else:
                res.append(ACEMASK_TABLE[idx][2])
            num_value = num_value ^ ACEMASK_TABLE[idx][0]
    return ', '.join(res)

def get_acemask(access_levels, is_object):
    if is_object:
        map_dict = CDMI_ACE_DB_MAPPING_DATA
    else:
        map_dict = CDMI_ACE_DB_MAPPING_COLL
    acemask = 0x0
    for lvl in access_levels:
        acemask |= map_dict[lvl]
    return acemask

def serialize_acl_metadata(obj):
    """obj = Collection or Resource"""
    # Create a dictionary of acl from object metadata (stored in Cassandra
    # lists)
    is_object = isinstance(obj, Resource)
    acls = defaultdict(list)
    if len(obj.read_access) > 0:
        for user in obj.read_access:
            acls[user].append("read")
    else:
        acls["AUTHENTICATED@"].append("read")
    
    if len(obj.edit_access) > 0:
        for user in obj.edit_access:
            acls[user].append("edit")
    else:
        acls["AUTHENTICATED@"].append("edit")
    
    if len(obj.write_access) > 0:
        for user in obj.write_access:
            acls[user].append("write")
    else:
        acls["AUTHENTICATED@"].append("write")
    
    if len(obj.delete_access) > 0:
        for user in obj.delete_access:
            acls[user].append("delete")
    else:
        acls["AUTHENTICATED@"].append("delete")
    
    mapped_md = []
    # Create a list of ACE from the dictionary we created
    for name, access_levels in acls.items():
        acl_md = OrderedDict()
        acl_md["acetype"] = "ALLOW"
        acl_md["identifier"] = name
        aceflags = ACEFLAG_OBJECT_INHERIT | ACEFLAG_CONTAINER_INHERIT
        acl_md["aceflags"] = aceflag_num_to_str(aceflags)
        acemask = get_acemask(access_levels, is_object)
        acl_md["acemask"] = acemask_num_to_str(acemask, is_object)
        mapped_md.append(acl_md)
    
    
    return { "cdmi_acl": mapped_md }


