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
import logging
import os
import socket
import sys, time
from Queue import Queue
from mimetypes import guess_type
from threading import Thread
import psycopg2
import docopt


from indigo import get_config
from indigo.models import initialise
from indigo.models.collection import Collection
from indigo.models.errors import (
    CollectionConflictError,
    ResourceConflictError,
    NoSuchCollectionError
)
from indigo.models.group import Group
from indigo.models.resource import Resource
from indigo.models.user import User
from indigo.util import split

def decode_str(s):
    if isinstance(s,unicode) : return s
    try: return s.decode('utf8')
    except UnicodeDecodeError:
        try:
            return s.decode('iso8859-1')
        except UnicodeDecodeError:
            s_ignore = s.decode('utf8', 'ignore')
            logger.error("Unicode decode error for {}, had to ignore character".format(s_ignore))
            return s_ignore

def db_init(cfg):
    """
    :return: returns a connection to the indigo database
    """
    credentials = dict(
        user = cfg.get('postgres',dict(user='indigo')),
        database = cfg.get('postgres',dict(database='indigo')),
        password = cfg.get('postgres',dict(password='indigo')),
        host = cfg.get('postgres',dict(host='localhost'))
    )
    return psycopg2.connect(**credentials)
# noinspection PyUnusedLocal

##########################
# Prepare -- fetch data from the source and populate the work queue in the database
##########################
def CreateFileNameSource(args,cfg) :
    """
    use the parameters to prepare an iterator that will deliver all the (suitably normalized) files to be injected
    :param args: command line args
    :param cfg:  global, persistent parameters
    :return: iterator
    """
    src = args['<source_directory>']
    mode = args['--mode']
    if mode not in ('walk','read','db') :
        print "mode must be one  'walk','read','db'  "
        sys.exit( 1 )
    prefix = args['--prefix']
    if not prefix : prefix = '/data'
    else : prefix = prefix.rstrip('/')
    if not src.startswith(prefix) :
        print src,' must be a subdirectory of the host data directory (--prefix=',prefix,')'
        print 'If you did not specify it, please do so'
        sys.exit( 1 )
    #########
    ## Set up a source that gets list of files from a file
    if  mode == 'read' :
        def walker( prefix , infile ):
            offset = len(prefix)
            for v in infile :
                v.strip()
                if not v.startswith(prefix) :
                    print v,' not in ', prefix ,'ignoring '
                    continue
                yield decode_str(v[offset:])
        if src == '-'  : reader = walker(prefix, sys.stdin)
        else : reader = walker( prefix,  open(src,'rU') )
        return reader
    ##################
    if  mode == 'walk' :
        if src == '-' :
            print 'Cannot walk stdin '
            return -1
        def walker(src) :
            offset = len(prefix)
            for dirname,_,files in os.walk(src,topdown=True,followlinks=True) :
                for fn in files :
                    yield decode_str(os.path.abspath(os.path.join(dirname[offset:],fn)))
         return walker(src)
    ##################
    if mode == 'db' :
        if prepare is True :
            print 'Cannot use --mode=db with prepare : prepare creates the DB'
            sys.exit(1)
        cnx = db_init(cfg)

        def walker(cs, table) :
            cs = cnx.cursor( 'rb1' , withhold = True)
            cs.execute("""SELECT path FROM "{0}" WHERE status = 'READY'""".format(table))
            for k in cs : yield k[0]
        return walker(cs,args['--dataset'])

def Prepare(args,cfg) :
    """
    prepare  [ --prefix=PREFIX --dataset=DATASET_NAME] --mode=MODE    (<source_directory>|-)
    :param args:
    :return:
    """

    cnx = db_init(cfg)
    cs = cnx.cursor()
    try: cs.execute("CREATE TYPE resource_status AS  ('READY','IN-PROGRESS','DONE','BROKEN','VERIFIED')")
    except : cs.connection.rollback()
    table = args.get('--dataset','resource')
    try:
        table = args.get('--dataset','resource')
        cs.execute('''CREATE TABLE IF NOT EXISTS "{0}"
            (path TEXT PRIMARY KEY, status resource_status DEFAULT 'READY', started timestamp,fs_sync boolean'''.format(table)
        cs.connection.commit()
    except Exception as e:
        print e
        return -1
    #### OK... dataset all set up , source all set up ... let's rip

    cs.execute("""
        PREPARE I1 ( text ) AS
           insert into "{0}" (path,status)
                  SELECT $1,'READY'::resource_status WHERE NOT EXISTS (SELECT TRUE FROM "{0}" where path = $1) A""".format(table))

    T0,cnt = time.time(),0
    reader = CreateFileNameSource(args , cfg)
    for path in reader:
        cnt +=1
        try:
            cs.execute("EXECUTE(I1,%s)")
            cs.connection.commit()
        except Exception as e :
            cs.connection.rollback()
            print path, ' not inserted '
        if cnt % 10000 == 0 :
            print "{0} entries added in {1} seconds, rate = {2:0.2f}".format(cnt,time.time()-T0,cnt/time.time()-T0)
    return 1

###################################
#### Actually inject into the underlying datastore
##################################
from collections import OrderedDict
class CollectionManager(OrderedDict ):
    def __init__(self,size = 1000) :
        super(Y, self).__init__(123)
        self.maxcount = size

    def cache(self,p,c):
        #### cache, promote and flush
        self[p] = c
        while len(self) > self.maxcount : self.popitem(last = False )   # trim excess entries - FIFO
        return c

    def collection(self, path):
        path = os.path.abspath(path)
        while path[0:2] == '//' : path = path[1:]
        try :
            c = self[path]
            self[path] = c              # Move back to top of FIFO ... so it stays here.
            return c
        except : pass

        c = Collection.find_by_path(path)
        if c :
            self.cache(path, c)
            return
        #### Ok, doesn't exist iterate up until it sticks

        p1,p2 = os.path.split(path)
        try :
            c = Collection.create(container=p1,name=p2)
            self.cache(path, c)
            return c
        except NoSuchCollectionError as e :
            parent = self.collection(p1)    # create parent ( and implicitly grandparents &c )
            return self.collection(path)    # and now should be ok...

def Inject(args, cfg) :
    files = CreateFileNameSource (args,cfg)
    if args['--mode'] == 'db' :
        class CP:
            def __init__(self,cfg):
                self.cnx = db_init(cfg)
                self.cs = self.cnx.cursor()
                self.cs.execute('PREPARE M1 (TEXT,resource_status) AS UPDATE "{0}" SET status=$2 WHERE path = $1 and status <> $2')
            def update(self,path,status):
                self.cs.execute('EXECUTE M1(%s,%s); commit',[path,status])
                return self.cs.rowcount
        updater = CP(cfg)
    else :
        class CP1:
            def __init__(self,cfg) : pass
            def update(self,path,status): return 1
        updater = CP1(cfg)

    def create_entry(self, rdict, context, do_load):
        self.queue.put((rdict.copy(), context.copy(), do_load))
        return

    collection_mgr = CollectionManager()

    def load( path ) :


        ##### AD-HOC #####
        import psycopg2
        cnx = db_init(cfg)
        # cnx1 = psycopg2.connect(user='indigo', password='indigo' , database='indigo' , host='localhost')

        cs1 = cnx.cursor()
        self.cs1 = cs1

        cs = None
        cnt = 0
        done = 900

        def worker_1(rcd, cs):
            import re
            match = re.compile(r'^file://(\d{1,3}\.){3}\d{1,3}/')
            try:
                if not isinstance(path, unicode): path = path.decode('utf8')
                if not isinstance(entry, unicode): entry = entry.decode('utf8')
            except:
                return

            path1 = os.path.join(path, entry)
            ref = Resource.find_by_path(path1)
            # validation and completion --
            if ref:
                if ref.url.startswith('file:'):
                    if ref.url.startswith('file://192.168.1.143'):
                        cs1.execute("update resource set status='DONE' , started=now() where resource_id = %s;commit",
                                    [resource_id])
                    else:
                        newurl = match.sub('file://192.168.1.143', ref.url)
                        if newurl != url:
                            ref.update(dict(url=newurl))
                            new_state = 'DONE'
                        else:
                            new_state = 'BROKEN'
                        cs1.execute("update resource set status=%s , started=now() where resource_id = %s;commit",
                                    [new_state, resource_id])
                    return
                ### Doesn't exist, so validate and inject
                fullpath = os.path.abspath(os.path.join(self.folder, path.lstrip('/')))
                if not os.path.isfile(fullpath):
                    print "file not found", path
                    return

                rdict = self.resource_for_file(fullpath)
                try:
                    p1 = fullpath[len(self.folder):]
                    if not p1.startswith('/'): p1 = '/' + p1
                    Resource.create(container=path, name=entry, url=u'file://{}{}'.format(self.local_ip, p1), **rdict)
                    try:
                        cs1.execute("update resource set status = 'DONE' where resource_id = %s  ; COMMIT  ",
                                    [resource_id])
                    except Exception as e:
                        print '1', e
                except ResourceConflictError as e:
                    cs1.execute("update resource set status = 'DONE' where resource_id = %s  ; COMMIT ", [resource_id])
                except Exception as  e:
                    print '2', p1, e

        def worker(q):
            cnx = psycopg2.connect(user='indigo', password='indigo', database='indigo', host='localhost')
            cs = cnx.cursor()
            while True:
                rcd = q.get()
                worker_1(rcd, cs)
                q.task_done()

        q = Queue(2000)
        threads = [Thread(target=worker, args=(q,)) for k in xrange(10)]
        for t in threads:
            t.daemon = True
            t.start()

        while done > 0:
            try:
                if cs and not cs.closed: cs.close()
            except:
                pass

            cs = cnx.cursor()

            cmd = '''
	             WITH  A as ( SELECT resource_id,container FROM resource where status = 'READY' LIMIT 1 )  
			UPDATE resource SET started = now()  , status = 'IN-PROGRESS' FROM  A
			WHERE resource.status = 'READY' AND A.container = resource.container
			RETURNING resource.resource_id,resource.container,resource.name'''

            cs.execute(cmd)
            done -= 1
            if cs.rowcount == 0:
                continue
            else:
                done = 900

            rcd = cs.fetchone()
            if not rcd: continue
            resource_id, path, entry = rcd
            print '[', path, ']'
            parent_path, name = split(path)
            current_collection = self.get_collection(path)
            if not current_collection:
                current_collection = self.create_collection(parent_path, name, path)

            q.put(rcd)

            for rcd in cs: q.put(rcd)
        q.join()

        cs.close()
        cs1.connection.commit()
        del cs


def load_config() :
    import configparser
    try:
        fn = os.path.expanduser('~/.indigo-ingest.config')
        parser = configparser.SafeConfigParser(fn)
    except :
        return None





    with open(fn,'rU') as fp :
        ingest_config =
def main():
    args_doc =  u'''
Ingest data files into Indigo from a directory tree

Usage:
    ingest inject   [ (--link|--copy) --prefix=PREFIX --local-ip=LOCAL_IP --dataset=DATASET_NAME --mode=MODE] (<source_directory>|-)
    ingest prepare  [ --prefix=PREFIX --dataset=DATASET_NAME] --mode=MODE    (<source_directory>|-)
    ingest validate [ --prefix=PREFIX --dataset=DATASET_NAME ]   [<source_directory>]
    ingest summary  [ --dataset=DATASET_NAME]

Arguments:
    inject    – actually move the data into the repository, either by linking or copying
    prepare   – harvest the filenames for later injections
    validate  – check that every filename marked as ingested is present in the repository
    summary   – list the counts of all the states

Options:
    --prefix=PREFIX         specify the directory that is the root of the local vault [ default: /data ]
    --localip=LOCAL_IP      specify the local ip address to use when linking [ default: '{ip}' ]
    --copy                  copy the data into Indigo  -- one of copy or link must be specified...
    --link                  link to the file
    --dataset=DATASET_NAME  a name for the prepared data [ default: resource ]
    --mode=MODE             one of walk,read or db — where to get list of files from.  Must be specified as walk or read for prepare [ default:db ]
'''

    __doc__ = args_doc
    cfg = load_config()
    local_ip = cfg.get('CASSANDRA_HOSTS', ('8.8.8.8',))
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    #### Try and determine local ip address...
    if local_ip : ip = local_ip[0]
    else : ip = None
    for k_ip in list(local_ip) :
        try:
            s.connect((k_ip, 7000))  # assuming that indigo will use port 7000 to connect to other nodes
            ip = s.getsockname()[0]
            if ip == k_ip and not ip.startswith('127.'): break
        except:
            continue
    if not ip: ip = '127.0.0.1'
    my_arg_doc = args_doc.format(ip=ip)
    s.close()
    #####
    if len(sys.argv) < 3 : args = 'prepare --mode=walk /data/Archive/ciber'.split()
    else: args = sys.argv[1:]
    args = docopt.docopt(my_arg_doc, args)
    if args['prepare'] :   return prepare(args,cfg)
    if args['summary'] : return summary(args,cfg)
    else :
        initialise(KEYSPACE='indigo', hosts=[ip, ])
        if args['inject'] : return inject(args,cfg)
        if args['validate'] : return validate(args,cfg)
        print 'Unknown subcommand ',sys.argv
        return None

main()
