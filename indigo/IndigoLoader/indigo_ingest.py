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
import socket
import sys
import time
from collections import OrderedDict

import configparser
import docopt

import FileNameSource
import writer
from FileNameSource import CreateFileNameSource
from indigo.models import initialise
from indigo.models.collection import Collection
from indigo.models.errors import (
    NoSuchCollectionError
)


# noinspection PyUnusedLocal

##########################
# Prepare -- fetch data from the source and populate the work queue in the database
##########################


def prepare(args, cfg):
    """
    prepare  [ --prefix=PREFIX --dataset=DATASET_NAME] --mode=MODE    (<source_directory>|-)
    :param args:
    :return:
    """
    reader = CreateFileNameSource(args, cfg)
    queue = FileNameSource.DBPrepare(args, cfg)
    cnt = 0
    for path in reader:
        cnt += 1
        try:
            queue.prepare(path)
        except Exception as e:
            print path, ' not inserted '
        if cnt % 10000 == 0:
            print "{0} entries added in {1} seconds, rate = {2:0.2f}".format(cnt, time.time() - T0,
                                                                             cnt / time.time() - T0)
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
        c = loader.put(path, args, cfg)
        files.confirm_completion(path)


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
def main():
    args_doc = u'''
Ingest data files into Indigo from a directory tree

Usage:
    ingest inject   [ (--link|--copy) --prefix=PREFIX --local-ip=LOCAL_IP --dataset=DATASET_NAME --mode=MODE --config=CONFIG ] (<source_directory>|-)
    ingest prepare  [ --prefix=PREFIX --dataset=DATASET_NAME  --config=CONFIG] --mode=MODE    (<source_directory>|-)
    ingest validate [ --prefix=PREFIX --dataset=DATASET_NAME  --config=CONFIG ]   [<source_directory>]
    ingest summary  [ --dataset=DATASET_NAME  --config=CONFIG]

Arguments:
    inject    – actually move the data into the repository, either by linking or copying
    prepare   – harvest the file names for later injections
    validate  – check that every file name marked as ingested is present in the repository
    summary   – list the counts of all the states

Options:
    --prefix=PREFIX         specify the directory that is the root of the local vault [ default: /data ]
    --localip=LOCAL_IP      specify the local ip address to use when linking [ default: '{ip}' ]
    --copy                  copy the data into Indigo  -- one of copy or link must be specified...
    --link                  link to the file
    --dataset=DATASET_NAME  a name for the prepared data [ default: resource ]
    --mode=MODE             one of walk,read or db — where to get list of files from.  Must be specified as walk or read for prepare [ default:db ]
    --config=CONFIG         location of config file [ default : ~/.indigo-ingest.cfg ]
'''

    __doc__ = args_doc
    cfg = load_config()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #### Try and determine local ip address...
    k_ip = cfg.get('cassandra','CASSANDRA_HOSTS')
    if k_ip : k_ip = re.split(r'[^\w\.]+',k_ip)
    else: k_ip = [ ]
    for k in k_ip:
        try:
                s.connect((k, 7000))  # assuming that indigo will use port 7000 to connect to other nodes
                ip = s.getsockname()[0]
                if not ip.startswith('127.'): break
        except:
                continue
    if not ip: ip = '127.0.0.1'
    s.close()
    ############################ might work
    my_arg_doc = args_doc.format(ip=ip)
    #####
    if len(sys.argv) < 3:
        args = 'prepare --mode=walk --prefix=/Users/johnburns/PycharmProjects /Users/johnburns/PycharmProjects'.split()
    else:
        args = sys.argv[1:]
    args = docopt.docopt(my_arg_doc, args)
    if args['prepare']:   return prepare(args, cfg)
    if args['summary']:
        raise NotImplementedError
        summary(args, cfg)
    else:
        try:
            cassandra_ip = ipcfg.get('cassandra', 'CASSANDRA_HOSTS')
            if cassandra_ip: cassandra_ip = cassandra_ip.split()
        except:
            cassandra_ip = ['127.0.0.1']
        cassandra_ip += ip

        initialise(keyspace='indigo', hosts=cassandra_ip)
        if args['inject']: return inject(args, cfg)
        if args['validate']:
            raise NotImplementedError
            return validate(args, cfg)
        print 'Unknown subcommand ', sys.argv
        return None


main()
