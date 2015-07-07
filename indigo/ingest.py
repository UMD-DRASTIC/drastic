import os
import sys
from mimetypes import guess_type

from indigo.models.search import SearchIndex
from indigo.models.blob import Blob
from indigo.models.user import User
from indigo.models.group import Group
from indigo.models.collection import Collection
from indigo.models.resource import Resource

SKIP = (".pyc",)

def do_ingest(cfg, args):
    if not args.user or not args.group or not args.folder:
        print "Group, User and Folder are all required for ingesting data"
        sys.exit(1)

    # Check validity of the arguments (do user/group and folder)
    # actually exist.
    user = User.find(args.user)
    if not user:
        print u"User '{}' not found".format(args.user)
        sys.exit(1)

    group = Group.find(args.group)
    if not group:
        print u"Group '{}' not found".format(args.group)
        sys.exit(1)

    path = os.path.abspath(args.folder)
    if not os.path.exists(path):
        print u"Could not find path {}".format(path)

    local_ip = args.local_ip
    skip_import = args.no_import

    ingester = Ingester(user, group, path, local_ip, skip_import)
    ingester.start()


class Ingester(object):

    def __init__(self, user, group, folder, local_ip='127.0.0.1', skip_import=False):
        self.groups = [group.id]
        self.user = user
        self.folder = folder
        self.collection_cache = {}
        self.skip_import = skip_import
        self.local_ip = local_ip

    def create_collection(self, name, path, parent):
        d = {}
        d['path'] = path
        d['name'] = name
        d['parent'] = parent
        d['write_access']  = self.groups
        d['delete_access'] = self.groups
        d['edit_access']   = self.groups

        c = Collection.create(**d)
        self.collection_cache[path] = c
        return c

    def get_collection(self, path):
        c = self.collection_cache.get(path, None)
        if c:
            return c

        c = Collection.find_by_path(path)
        if c:
            self.collection_cache[path] = c
            return c

        return None

    def resource_for_file(self, path):
        d = {}
        d['name'] = path.split('/')[-1]
        d['file_name'] = path.split('/')[-1]

        t, _ = guess_type(path)
        _, ext = os.path.splitext(path)

        d['mimetype'] = t
        d['type'] = ext[1:].upper()
        d['size'] = os.path.getsize(path)

        #d['read_access'] = self.groups
        d['write_access']  = self.groups
        d['delete_access'] = self.groups
        d['edit_access']   = self.groups
        return d


    def start(self):
        """
        Walks the folder creating collections when it finds a folder,
        and resources when it finds a file. This is done sequentially
        so multiple copies of this program can be run in parallel (each
        with a different root folder).
        """


        root_collection = Collection.get_root_collection()
        if not root_collection:
            root_collection = Collection.create(name="Home", path="/")
        self.collection_cache["/"] = root_collection

        paths = []
        for (path, dirs, files) in os.walk(self.folder, topdown=True):
            if '/.' in path: continue # Ignore .paths
            paths.append(path)

        def name_and_parent_path(p):
            parts = p.split('/')
            return parts[-2], '/'.join(parts[:-2]) + "/"

        paths = [p[len(self.folder):] + "/" for p in paths]
        paths.sort(key=len)

        for path in paths:
            name, parent_path = name_and_parent_path(path)

            print "Processing {} with name '{}'".format(path, name)
            print "  Parent path is {}".format(parent_path)

            if name:
                parent = self.get_collection(parent_path)
                print "  Parent collection is {}".format(parent)

                current_collection = self.get_collection(path)
                if not current_collection:
                    current_collection = self.create_collection(name, path, parent.id)
            else:
                current_collection = root_collection

            # Now we can add the resources from self.folder + path
            for entry in os.listdir(self.folder + path):
                fullpath = self.folder + path + entry

                if entry.startswith("."): continue
                if entry.endswith(SKIP): continue
                if not os.path.isfile(fullpath): continue

                rdict = self.resource_for_file(fullpath)
                rdict["container"] = current_collection.id

                resource = None
                existing = Resource.objects.filter(container=current_collection.id).all()
                for e in existing:
                    if e.name == rdict['name']:
                        resource = e
                        break

                if not resource:
                    # Create the resource
                    print "  Creating the resource"
                    resource = Resource.create(**rdict)

                if not resource.url:
                    # Upload the file content as blob and blobparts!
                    # TODO: Allow this file to stay where it is and reference it
                    # with IP and path.
                    if self.skip_import:
                        # Specify a URL for this resource to point to the agent on this
                        # machine.  It's important that the agent is configured with the
                        # same root folder as the one where we import.
                        print "    Creating URL entry for resource"
                        url = "file://{}{}".format(self.local_ip, path + entry)
                        resource.update(url=url)

                    else:
                        # Push the file into Cassandra
                        with open(fullpath, 'r') as f:
                            print "    Creating blob for resource..."
                            blob = Blob.create_from_file(f, rdict['size'])
                            if blob:
                                resource.update(url="cassandra://{}".format(blob.id))

                SearchIndex.reset(resource.id)
                SearchIndex.index(resource, ['name', 'metadata'])







