"""Graph Metadata Model
Used by collections and resources to store user metadata in the graph database.

"""
from drastic import get_config
from gremlin_python.structure.graph import Graph
import logging
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.strategies import *
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"

cfg = get_config()
gremlin_host = cfg.get('GREMLIN_HOST', '127.0.0.1')
gremlin_port = cfg.get('GREMLIN_PORT', 8182)
gremlin_uri = u'ws://{0}:{1}'.format(gremlin_host, gremlin_port)
gremlin_graph = 'drasticgraph.g'


def patch_graph_metadata():
    """Adds and removes triples from existing data."""


def put_graph_metadata(uuid, name, metadata, container_uuid):
    """Replaces existing user triples for a single subject."""
    logging.debug(u'PUT {0} RDF metadata fields for {1}'.format(len(metadata.keys()), uuid))
    uri = "uuid:{0}".format(uuid)
    get_g().V().has('resource', 'URI', uri).drop().count().next()
    t = get_g().addV('resource')
    t = t.property('URI', uri)
    t = t.property('graph', uri)
    t = t.property('name', name)
    # t = add_literal_edge(t, uri, 'dcterms:title', name)  # TODO check against FCREPO 4.x
    for key, value in metadata.iteritems():
        # Don't store metadata without value
        if value is None:  # numeric zero is a valid value
            continue
        t = t.property(key, value)  # key/values as properties
        # TODO add default namespace for keys that are plain tokens
        # t = add_literal_edge(t, uri, key, value)

    # Add contains Edge
    if container_uuid is not None:
        container_uri = "uuid:{0}".format(container_uuid)
        c = get_g().V().has('resource', 'URI', container_uri)
        # TODO fully qualify URIs
        t = t.addE('contains').from_(c)

    t.next()
    logging.debug(u'Created resource vertex for {0}'.format(uuid))


def add_literal_edge(traversal, graph_uri, predicate_uri, value):
    traversal = traversal.addE('statement').property('URI', predicate_uri)
    traversal = traversal.to(get_g().addV('literal').property('graph', graph_uri)
                             .property('type', 'xsd:string').property('value', value)).outV()
    return traversal


def delete_graph_metadata(uuid):
    """Drop graph Vertex for resource and it's properties"""
    count = get_g().V().has('resource', 'drastic:uuid', uuid).drop().count().next()
    logging.debug(u'Dropped graph metadata for {0}, count {1} (should be 0)'
                  .format(uuid, str(count)))


def get_g():
    connection = DriverRemoteConnection(gremlin_uri, gremlin_graph)
    graph = Graph()
    g = graph.traversal().withRemote(connection)
    return g
