import argparse
import sys

from indigo import get_config
from indigo.models import initialise, sync, destroy

def parse_arguments():
    parser = argparse.ArgumentParser(description='Interact with the indigo system')
    parser.add_argument('command', type=str, metavar="N", nargs="+",
                       help='The command to run')
    parser.add_argument('--config', '-c', dest='config', action='store',
                       help='Specify the location of the configuration file')

    return parser.parse_args()


def create(cfg):
    initialise(cfg.get("KEYSPACE", "indigo"))
    sync()

def zap(cfg):
    keyspace = cfg.get("KEYSPACE", "indigo")
    initialise(keyspace)
    destroy(keyspace)

def user_add(cfg, username=None):
    from indigo.models import User
    from getpass import getpass

    if not username:
        username = raw_input("Please enter the user's username: ")
    else:
        username = username[0]

    admin = raw_input("Is this an administrator? [y/N] ")

    # Check if user exists and bail if so
    initialise(cfg.get("KEYSPACE", "indigo"))
    user = User.objects.filter(username=username).first()
    if user:
        print "ERROR: Username {} is already in use".format(username)
        sys.exit(1)

    email = raw_input("Please enter the user's email address: ")
    password = getpass("Please enter the user's password: ")
    User.create(username=username, password=password, email=email,administrator=(admin.lower()=='y'))

    print "Success: User with username {} has been created".format(username)


def main():
    args = parse_arguments()
    cfg = get_config(args.config)

    command = args.command[0]
    if command == 'create':
        create(cfg)
    elif command == 'user-add':
        user_add(cfg, args.command[1:])
    elif command == 'zap':
        zap(cfg)
