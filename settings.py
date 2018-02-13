import socket
import os


def ips(hostnames):
    result = []
    hostnames = hostnames.split(',')
    for v in hostnames:
        try:
            ip = socket.gethostbyname(v)
            result.append(ip)
        except Exception as e:
            print(e)
    return result

KEYSPACE = os.getenv('DRASTIC_KEYSPACE', 'drastic')
LOG_LEVEL = os.getenv('DRASTIC_LOG_LEVEL', 'WARN')
CASSANDRA_HOSTS = ips(os.getenv('CASSANDRA_HOSTNAMES', 'cassandra-1'))
REPLICATION_FACTOR = int(os.getenv('CASSANDRA_REPLICATION_FACTOR', '2'))
CONSISTENCY_LEVEL = int(os.getenv('CASSANDRA_CONSISTENCY_LEVEL', '2'))
