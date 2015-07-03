import argparse
import sys

from indigo import get_config
from indigo.models.errors import UniqueException
from indigo.models import initialise, sync, destroy

from indigo.ingest import do_ingest

def parse_arguments():
    parser = argparse.ArgumentParser(description='Interact with the indigo system')
    parser.add_argument('command', type=str, metavar="N", nargs="+",
                       help='The command to run')
    parser.add_argument('--config', '-c', dest='config', action='store',
                       help='Specify the location of the configuration file')
    parser.add_argument('--group', '-g', dest='group', action='store',
                       help='Specify the group name for ingestion of data')
    parser.add_argument('--user', '-u', dest='user', action='store',
                       help='Specify the username for ingestion of data')
    parser.add_argument('--folder', dest='folder', action='store',
                       help='Specify the root folder to ingest from on disk')
    return parser.parse_args()


def create(cfg):
    initialise(cfg.get("KEYSPACE", "indigo"))
    sync()

def zap(cfg):
    keyspace = cfg.get("KEYSPACE", "indigo")
    initialise(keyspace)
    destroy(keyspace)

def user_list(cfg):
    from indigo.models import User
    for user in User.objects.all():
        print "Username: {}, ID: {}".format(user.username, user.id)

def user_add(cfg, username=None):
    from indigo.models import User
    from getpass import getpass

    if not username:
        username = raw_input("Please enter the user's username: ")
    else:
        username = username[0]

    admin = raw_input("Is this an administrator? [y/N] ")

    user = User.objects.filter(username=username).first()
    if user:
        print "ERROR: Username {} is already in use".format(username)
        sys.exit(1)

    email = raw_input("Please enter the user's email address: ")
    password = getpass("Please enter the user's password: ")
    User.create(username=username, password=password, email=email,administrator=(admin.lower()=='y'))

    print "Success: User with username {} has been created".format(username)

def group_add(cfg, args):
    from indigo.models import Group, User
    if not args or not len(args) == 2:
        print "Error: Group Name and Username are required parameters"
        sys.exit(0)

    name, username = args
    user = User.find(username)
    try:
        group = Group.create(name=name, owner=user.id)
    except UniqueException:
        print "A group with that name already exists"
        return

    print "Created group '{}' with id: {}".format(name, group.id)

def group_delete(cfg, args):
    from indigo.models import Group, User
    if not args or not len(args) == 1:
        print "Error: Group Name is a required parameters"
        sys.exit(0)

    name = args[0]
    group = Group.find(args[0])
    group.delete()

    print "Deleted group '{}' with id: {}".format(group.name, group.id)


def group_add_user(cfg, args):
    from indigo.models import Group, User
    if not args or not len(args) == 2:
        print "Error: Group Name and Username are required parameters"
        sys.exit(0)

    group_name, username = args
    user = User.find(username)
    group = Group.find(group_name)
    if not group.id in user.groups:
        user.groups.append(group.id)
        user.update(groups=user.groups)

    print "Added {} to {}".format(user.username, group.name)

def group_list(cfg):
    from indigo.models.group import Group
    for group in Group.objects.all():
        print "Name: {}, ID: {}".format(group.name, group.id)
        for user in group.get_users():
            print ".ID: {}\tUsername: {}\tAdministrator:{}\tOwner: {}".format(
                user.id, user.username, ("N", "Y")[user.administrator],
                ("N", "Y")[user.id==group.owner])

def main():
    args = parse_arguments()
    cfg = get_config(args.config)

    initialise(cfg.get("KEYSPACE", "indigo"))

    command = args.command[0]
    if command == 'create':
        create(cfg)
    elif command == 'user-create':
        user_add(cfg, args.command[1:])
    elif command == 'user-list':
        user_list(cfg)
    elif command == 'group-create':
        group_add(cfg, args.command[1:])
    elif command == 'group-list':
        group_list(cfg)
    elif command == 'group-add-user':
        group_add_user(cfg, args.command[1:])
    elif command == 'group-delete':
        group_delete(cfg, args.command[1:])
    elif command == 'zap':
        zap(cfg)
    elif command == 'ingest':
        do_ingest(cfg, args)
