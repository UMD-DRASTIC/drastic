# coding=utf-8
"""Ingest workflow management tool
    FileNameSource Class

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

import abc
import os
import sys

import psycopg2


class FileNameSource:
    def __init__(self):  pass

    def __iter__(self):  return self

    @abc.abstractmethod
    def next(self): pass

    def confirm_completion(self, path):
        return True


class FileList(FileNameSource):
    def __init__(self, args, cfg):
        FileNameSource.__init__(self)
        src = args['<source_directory>']
        self.fp = sys.stdin if src == '-' else  open(src, 'rU')

        self.prefix = args['--prefix']
        self.offset = len(self.prefix)

    def next(self):
        v = self.fp.next().strip()
        if not v.startswith(self.prefix):
            print v, ' not in ', self.prefix, 'ignoring '
            return
        return decode_str(v[self.offset:])


class DirectoryWalk(FileNameSource):
    def __init__(self, args, cfg):
        FileNameSource.__init__(self)
        src = args['<source_directory>']
        if src == '-':
            print ' Incompatible mode -- Cannot Walk stdin '
            raise ValueError
        self.prefix = args['--prefix']
        self.offset = len(self.prefix)
        self.walker = os.walk(src, topdown=True, followlinks=True)
        self.dirname = None
        self.files = None

    def next(self):
        while not self.dirname or not self.files:
            self.dirname, _, self.files = self.walker.next()
        return os.path.join(self.dirname[self.offset:], self.files.pop())


class DB:
    def __init__(self, args, cfg):
        defaults = (('user', 'indigo'), ('database', 'indigo'), ('password', 'indigo'), ('host', 'localhost'))
        credentials = dict(user=cfg.get('postgres', 'user'),
                           database=cfg.get('postgres', 'database'),
                           password=cfg.get('postgres', 'password'),
                           host=cfg.get('postgres', 'host'))

        for k, v in defaults:
            if not credentials[k]: credentials[k] = v

        self.credentials = credentials
        self.cnx = psycopg2.connect(**credentials)
        self.cs1 = self.cnx.cursor()

        table = args.get('--dataset', 'resource')
        if not table: table = 'resource'

        self.tablename = table

        ### Do JIT set up of other queries....
        self.update_status = False
        self.db_initialized = False

    def summary(self):
        cmd = '''SELECT status,count(*) from "{0}" group by status order by status '''.format(self.tablename)
        try:
            self.cs1.execute(cmd)
            for v in self.cs1: print '{0:-10s}\t{1:,}'.format(*v)
        except Exception as e:
            print e

    def _setup_db(self, table):
        cs = self.cnx.cursor()
        # Create the status Enum
        try:
            cs.execute("CREATE TYPE resource_status AS ENUM ('READY','IN-PROGRESS','DONE','BROKEN','VERIFIED')")
        except:
            cs.connection.rollback()
        #

            cmds = [
                '''CREATE TABLE IF NOT EXISTS "{0}" (
                        path TEXT PRIMARY KEY,
                        status resource_status DEFAULT 'READY',
                        started timestamp,
                        fs_sync boolean)''',
                '''CREATE INDEX "IDX_{0}_01_status"  ON "{0}" (status ) WHERE status <> 'DONE' ''',
                '''CREATE INDEX "IDX_{0}_01_fs_sync" ON "{0}" (fs_sync) WHERE fs_sync is not True''']
            for cmd in cmds:
                try:
                    cs.execute(cmd.format(table))
                    cs.connection.commit()
                except Exception as e:
                    cs.connection.rollback()


class DBPrepare(DB):
    """
    Class to be used when preparing.
    """

    def __init__(self, args, cfg):
        DB.__init__(self, args, cfg)
        self.prefix = (args['--prefix'])
        self.offset = len(self.prefix)
        self.cs = self.cnx.cursor('AB1', withhold=True)
        self._setup_db(self.tablename)
        cmd = '''PREPARE I1 ( text ) AS insert into "{0}" (path,status)
            SELECT $1,'READY'::resource_status WHERE NOT EXISTS (SELECT TRUE FROM "{0}" where path = $1)'''
        self.cs1.execute(cmd.format(self.tablename))

    def prepare(self, path ):
        self.cs1.execute("EXECUTE I1(%s); commit", [path])
        return True



class DBQuery(FileNameSource, DB):
    """
    Class to be used to get file names when injecting.
    """

    def __init__(self, args, cfg):
        DB.__init__(self,args,cfg)
        FileNameSource.__init__(self)
        self.prefix = (args['--prefix'])
        self.offset = len(self.prefix)
        self.fetch_cs = self.cnx.cursor()
        cmd = '''PREPARE F1 (integer) AS SELECT path FROM "{0}" where status = 'READY' LIMIT $1 '''.format(self.tablename)
        self.fetch_cs.execute(cmd)
        self.fetch_cs.execute('EXECUTE F1 (1000)')
        # And prepare the update status cmd
        ucmd = '''PREPARE M1 (TEXT,resource_status) AS UPDATE "{0}" SET status='DONE' WHERE path = $1 and status <> $2 '''.format(
            self.tablename)
        self.cs1.execute(ucmd)
        # And retreive the values for the status
        self.cs1.execute('''SELECT unnest(enum_range(NULL::resource_status))''')
        self.status_values = set( ( k[0] for k in self.cs1.fetchall() ))
        return

    def confirm_completion(self, path, status = 'DONE'):
        if status not in self.status_values :
            if status == 'FAILED' : status = 'BROKEN'
            else : raise ValueError("bad value for enum -- {} : should be {}".format(status,self.status_values) )
        ####
        try:
            self.cs1.execute('EXECUTE M1(%s,%s)', [path,status])
            updates = self.cs1.rowcount
            self.cs1.connection.commit()
            return True
        except Exception as e:
            print 'failed to update status for ', path,'\n',e
            self.cs1.connection.rollback()
            return False

    def next(self):
        """
        :return: next path from DB that is ready...

        This function will re-issue the Select when the current one is exhausted.
        This attempts to avoid two many locks on two many records.

        """
        k = self.fetch_cs.fetchone()
        #
        if not k:
            self.fetch_cs.execute('EXECUTE F1 (1000)')
            k = self.fetch_cs.fetchone()
        #
        if k: return k[0].decode('utf-8')
        raise StopIteration


def CreateFileNameSource(args, cfg):
    """
    use the parameters to prepare an iterator that will deliver all the (suitably normalized) files to be injected
    :param args: command line args
    :param cfg:  global, persistent parameters
    :return: iterator
    """
    src = args['<source_directory>']
    prefix = args['--prefix']
    if not prefix:
        prefix = '/data'
    else:
        prefix = prefix.rstrip('/')

    if not src.startswith(prefix):
        print src, ' must be a subdirectory of the host data directory (--prefix=', prefix, ')'
        print 'If you did not specify it, please do so'
        sys.exit(1)
    #########
    ## Set up a source that gets list of files from a file
    if args['--read'] : return FileList(args, cfg)
    if args['--walk']:  return DirectoryWalk(args, cfg)
    if args['--postgres'] :  return DBQuery(args, cfg)
    if args['--sqlite3'] :
        raise NotImplementedError


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
