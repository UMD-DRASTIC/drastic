import socket
import os


def ips(hostnames):
    result = []
    hostnames = hostnames.split(',')
    for v in hostnames:
        if v == '':
            continue
        try:
            socket.inet_aton(v)  # test legal IP
            result.append(v)
        except socket.error as e1:  # Not legal IP
            print(e1)
            try:
                ip = socket.gethostbyname(v)  # returns the IP unchanged for an IP
                result.append(ip)
            except Exception as e:
                print(e)
    return result

KEYSPACE = os.getenv('DRASTIC_KEYSPACE', 'drastic')
LOG_LEVEL = os.getenv('DRASTIC_LOG_LEVEL', 'WARN')
CASSANDRA_HOSTS = ips(os.getenv('CASSANDRA_HOSTNAMES', 'cassandra-1'))
REPLICATION_FACTOR = int(os.getenv('CASSANDRA_REPLICATION_FACTOR', '2'))
CONSISTENCY_LEVEL = int(os.getenv('CASSANDRA_CONSISTENCY_LEVEL', '2'))

if __name__ == '__main__':
    print('hosts: {0}'.format(str(CASSANDRA_HOSTS)))
