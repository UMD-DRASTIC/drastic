"""ACL constants and functions

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

from indigo import get_config
from indigo.models import initialise
from indigo.models.user import User


def main():
    
    cfg = get_config(None)
    initialise(cfg.get('KEYSPACE', 'indigo'),
               hosts=cfg.get('CASSANDRA_HOSTS', ('127.0.0.1', )),
               repl_factor=cfg.get('REPLICATION_FACTOR', 1))
    
    users = User.objects.all()
    for u in users:
        if not u.name:
            u.name = u.username
            u.save()
    
    print "All done, you can now remove username in models.user.py and alter"
    print "the table in Cassandra (ALTER TABLE user DROP username;)"


if __name__ == "__main__":
    main()