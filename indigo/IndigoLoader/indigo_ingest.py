# coding=utf-8
"""Ingest workflow management tool

Copyright 2015 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import os
import re
import sys
import time

import configparser
import docopt

import FileNameSource
import writer
from FileNameSource import CreateFileNameSource
from indigo.models import initialise


# noinspection PyUnusedLocal

##########################
# Prepare -- fetch data from the source and populate the work queue in the database
##########################


def prepare(args, cfg):
    """
    prepare  [ --prefix=PREFIX --dataset=DATASET_NAME] {mode}    (<source_directory>|-)
    :param args:
    :return:
    """
    reader = CreateFileNameSource(args, cfg)
    queue = FileNameSource.DBPrepare(args, cfg)
    cnt = 0
    T0 = time.time()
    for path in reader:
        cnt += 1
        try:
            queue.prepare(path)
        except Exception as e:
            print path, ' not inserted '
        if cnt % 10000 == 0:
            print "{0} entries added in {1} seconds, rate = {2:0.2f}".format(cnt, time.time() - T0,
                                                                             cnt / time.time() - T0)
    print "{0} entries added in {1} seconds, rate = {2:0.2f}".format(cnt, time.time() - T0,  cnt / time.time() - T0)
    return 1


###################################
#### Actually inject into the underlying datastore
##################################



def inject(args, cfg):
    files = CreateFileNameSource(args, cfg)
    ############# function to create link
    # Specify the loader ...
    if args['--copy']:
        loader = writer.CopyWriter(args, cfg)
    elif args['--link']:
        loader = writer.LinkWriter(args, cfg)
    elif args['--send']:
        loader = writer.SendWriter(args, cfg)
    else:
        loader = writer.load  # default is to load.

    for path in files:
        update = 'DONE'
        try:
            c = loader.put(path)
        except Exception as e :
            update = 'FAILED'            # do NOT confirm
        files.confirm_completion(path,update)


############### Utility Routines ##################
def load_config(cfg=None):
    k = [os.path.expanduser(f) for f in ['~/.indigo-ingest.config', cfg] if bool(f)]

    defaults = dict(KEYSPACE='indigo', user='indigo', password='indigo', host='localhost')
    cfg = configparser.SafeConfigParser(defaults)
    cfg.read(*k)
    return cfg


def decode_str(s):
    """
    :param s: string to be converted to unicode
    :return: unicode version
    """
    if isinstance(s, unicode): return s
    try:
        return s.decode('utf8')
    except UnicodeDecodeError:
        try:
            return s.decode('iso8859-1')
        except UnicodeDecodeError:
            s_ignore = s.decode('utf8', 'ignore')
            return s_ignore


############### Utility Routines ##################

args_doc = \
u'''Ingest data files into Indigo from a directory tree

Usage:
     ingest prepare  (--walk|--read) [--config=CONFIG  --dataset=DATASET_NAME --prefix=PREFIX  (--postgres|--sqlite)]  (<source_directory>|-)
     ingest inject   (--copy|--link) [--config=CONFIG  --dataset=DATASET_NAME --prefix=PREFIX --localip=LOCAL_IP] (--walk|--read) (<source_directory>|-)
     ingest inject   (--copy|--link) [--config=CONFIG  --dataset=DATASET_NAME --prefix=PREFIX --localip=LOCAL_IP] (--postgres|--sqlite)
     ingest validate (--dataset=DATASET_NAME|<source_directory>)
     ingest summary  [--config=CONFIG  --dataset=DATASET_NAME ]   [(--postgres|--sqlite)]


 Arguments:
     inject    -- actually move the data into the repository, either by linking or copying
     prepare   -- harvest the file names for later injections',
     validate  -- check that every file name marked as ingested is present in the repository
     summary   -- list the counts of all the states

 Options:
    --config=CONFIG          # location of config file [ default : ~/.indigo-ingest.cfg ]
    --prefix=PREFIX          # specify the directory that is the root of the local vault [ default: /data ]
    --dataset=DATASET_NAME   # a name for the prepared data [ default: resource ]
    --localip=LOCAL_IP       # specify the ip address to use when linking, i.e. where the files will actually reside [ default: 127.0.0.1 ]
    --copy                   # copy the data into Indigo  -- one of copy or link must be specified...
    --link                   # link to the file from Indigo, i.e. reference, not copy.
    --walk                   # walk the source tree to get the list of files
    --read                   # read the list of filenames from a file or stdin
    --postgres               # read file names from or store filenames to a postgres database
    --sqlite                 # read file names from or store filenames to a postgres database

'''
help_text=u'''
The basic workflow provided by this utility is to acquire a list of file names and inject
them into the Indigo store.

The list of file names can be stored into a persistent work queue in a postgres or sqlite3
database (using the prepare phase).  It is strongly recommended that this phase be used for anything
greater than a few hundred files, since it provides the option to restart, to have multiple injections
running in parallel, and to be able to track status, performance and failed injections.  The use of the
––dataset option allows you to name the work queue, which may be useful — the default is 'resource'


The injection phase can draw from a list of file names in a file, from a walk of the live directory, or from
a database work queue created in the prepare phase.  In the latter case, multiple injections can proceed
in parallel, which will typically improve performance linearly with the number of parallel processes.

The config file allows you to set up the credentials and locations of resources, and consists of 2 sections
e.g.

[cassandra]
CASSANDRA_HOSTS = 192.168.56.101, 192.168.56.102

[postgres]
username = indigo
password = indigo
database = indigo
host = 192.168.56.101

N.B. It typically only makes sense to link to files that are logically or physically  "on" an indigo node, since access
to them will go via the indigo server.  i.e. this is a local file access.

Remote file access is not yet supported. Moreover, for copy and link ( not send ) , the injection bypasses normal
indigo access control, and so typically can only work on an indigo server.  Send can work from anywhere, and supports
embedded files, but not linked.

'''
# docopt is definitely broken on long complex strings  ... so clean up string a little.
args_doc_tidied = args_doc.split('\n')
args_doc_tidied = u'\n'.join([ k.split('#')[0]  for k in args_doc_tidied])

def main():
    __doc__ = args_doc
    cfg = load_config()
    ############################ might work
    args = sys.argv[1:]
    ##### Test fragment ... remove
    ## TODO: remove test fragment
    if len(sys.argv) < 3:
        args = 'inject --copy --postgres --prefix=/Users/johnburns/PycharmProjects --dataset=resource'.strip().split()
        args = '''prepare --walk  --postgres  --prefix=/Users/johnburns/PycharmProjects /Users/johnburns/PycharmProjects'''.split()
    ### End Remove


    try : args = docopt.docopt(args_doc_tidied, args)
    except docopt.DocoptExit as e :
        print args_doc.encode('utf-8'),
        print help_text.encode('utf-8')
        print 'Command Line:\n' + (' '.join(args))
        raise
    except docopt.DocoptLanguageError as e :
        print e
        raise
    if args['prepare']:   return prepare(args, cfg)
    if args['summary']:
        raise NotImplementedError
        summary(args, cfg)
    else:
        try:
            cassandra_ip = cfg.get('cassandra', 'CASSANDRA_HOSTS')
            if cassandra_ip: cassandra_ip = re.split(r'[^\w\.]+',cassandra_ip  )
        except:
            cassandra_ip = ['127.0.0.1']

        initialise(keyspace='indigo', hosts=cassandra_ip)
        if args['inject']: return inject(args, cfg)
        if args['validate']:
            raise NotImplementedError
            return validate(args, cfg)
        print >>sys.stderr, my_arg_doc, "\n Unknown subcommand \n", sys.argv
        return None


main()
