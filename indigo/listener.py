# Listener
#
# Copyright 2015 Archive Analytics Solutions
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Listener
Listens for CRUD events to the database and executes user-defined scripts based on those events.

Usage:
  listener.py <script_directory> <script_collection> [--quiet | --verbose]
  listener.py -h | --help

Options:
  -h, --help  Show this message.
  --verbose   Increase logging output to DEBUG level.
  --quiet     Decrease logging output to WARNING level.

"""

import logging
import json
import os
import StringIO
import subprocess
import sys
import signal

# noinspection PyPackageRequirements
from docopt import docopt
import gevent
from gevent import monkey; monkey.patch_all()
import paho.mqtt.client as mqtt
# noinspection PyPackageRequirements
import magic

from indigo import get_config
import log
from models import (
    initialise,
    Collection,
    ListenerLog,
    Resource,
)
from util import meta_cassandra_to_cdmi

scripts = dict()
MAX_PROC_TIME = 12


# noinspection PyUnusedLocal
def on_connect(client, userdata, flags, rc):
    logger.info('Connected to MQTT broker with result code {0}'.format(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe('#')
    logger.info('Subscribing to all topics')
    logger.info('Listening on "{0}" for scripts'.format(script_directory_topic))


# noinspection PyUnusedLocal
def on_disconnect(client, userdata, rc):
    if rc != 0:
        logger.warning('Unexpected disconnection from MQTT broker with result code {0}'.format(rc))
    else:
        logger.info('Disconnected from MQTT broker')


# noinspection PyUnusedLocal
def on_message(client, userdata, msg):
    if mqtt.topic_matches_sub(script_directory_topic, msg.topic):
        # Something happens in the scripts folder, update scripts
        path = msg.topic.split('/')
        operation = path[0]
        script = '/'.join(path[3:])
        payload = json.loads(msg.payload)
        
        try:
            trigger_topic = payload['post']['metadata']['topic']
        except KeyError:
            logger.warning('Script "{0}" does not have a trigger topic. Ignoring.'.format(script))
            logger.debug('Payload: {}'.format(json.dumps(payload)))
            return

        if operation not in ('delete', 'create', 'update', 'update_object', 'update_metadata'):
            logger.warning('Unknown operation "{0}" for script "{1}". Ignoring.'.format(operation, script))
            return

        script_full_path = os.path.join(script_directory, script)
        script_path = os.path.dirname(script_full_path)
        script_file_name = os.path.basename(script_full_path)

        if operation is 'delete':
            logger.info('{1} script "{0}" for topic "{2}"'.format(script, operation, trigger_topic))
            del scripts[script]
            os.unlink(os.path.join(script_path, script_file_name))
            return
        
        # TODO: Refactor this and combine it with the scan_script_collection() function.
        resource = Resource.find("{}/{}".format(payload['post']['container'],
                                                payload['post']['name']))
        
        script_contents = StringIO.StringIO()

        for chunk in resource.chunk_content():
            script_contents.write(chunk)

        script_type = magic.from_buffer(script_contents.getvalue())
        logger.debug('Script "{0}" is apparently of type "{1}"'.format(script, script_type))

        if script_type in ('Python script, ASCII text executable', 'ASCII text'):
            logger.info('{1} script "{0}" for topic "{2}" at "{3}"'.format(script, operation,
                                                                           trigger_topic, script_full_path))
            logger.debug('Payload: {}'.format(json.dumps(payload)))

            if not os.path.exists(script_path):
                os.makedirs(script_path)

            with open(script_full_path, 'w') as f:
                f.write(script_contents.getvalue())

            scripts[script] = {'topic': trigger_topic, 'path': script_full_path}
        else:
            logger.warning('File "{0}" appears not to be a Python script. Ignoring.'.format(script))
    else:
        useful_topic = False

        for script, sub in scripts.iteritems():
            if mqtt.topic_matches_sub(sub['topic'], msg.topic):
                useful_topic = True
                execute_script(sub['path'], msg.topic, msg.payload)

        if not useful_topic:
            logger.debug('Topic: {}'.format(msg.topic))


def kill_container(pid):
    logger.debug('Attempting to kill PID: {0}'.format(pid))
    try:
        os.kill(pid, signal.SIGKILL)
        logger.warning('Process {0} was killed after {1} seconds. '
                       'This might be just because the process is zombified '
                       'and probably isn\'t anything to worry about'.format(pid, MAX_PROC_TIME))
    except OSError:
        logger.debug('Process {0} has already terminated'.format(pid))


def mqtt_loop():
    while True:
        mqtt_client.loop()
        gevent.sleep(0)


def execute_script(script, topic, payload):
    logger.info('execute script "{0}" for topic "{1}"'.format(script, topic))
    absolute_path = os.path.abspath(script)
    directory = os.path.dirname(absolute_path)
    filename = os.path.basename(absolute_path)

    # TODO: Limit the available memory for each instance.
    # TODO: Open up certain ports.
    docker_cmd = 'docker run --rm --net="host" -i -v {0}:/scripts alloy_python'.format(directory)
    logger.debug('{0} {1} {2} {3}'.format(docker_cmd, filename, topic, payload))
    params = docker_cmd.split()
    params.extend((filename, topic, payload))
    proc = subprocess.Popen(params,
                            shell=False,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    gevent.spawn_later(MAX_PROC_TIME, kill_container, proc.pid)
    proc.stdin.write(payload)
    
    stdout, stderr = proc.communicate()
    log = ListenerLog.create(script_name="/scripts/{}".format(filename),
                             stdout=stdout,
                             stderr=stderr)
    proc.stdin.close()


def scan_script_collection(directory):
    logging.info('Scanning "{0}" for scripts'.format(directory))
    collection = Collection.find(directory)

    if collection is None:
        logging.warning('There are no scripts to scan because I am unable '
                        'to find the collection "{0}" in the database.'.format(directory))
        return

    child_container, child_dataobject = collection.get_child()
    resource_count = len(child_dataobject)
    logging.info('{0} scripts found in collection "{1}"'.format(resource_count, directory))

    # TODO: Refactor this and combine it with the on_message() function.
    
    for resource_name in child_dataobject:
        resource = Resource.find("{}/{}".format(directory, resource_name))
        script_contents = StringIO.StringIO()

        for chunk in resource.chunk_content():
            script_contents.write(chunk)

        trigger_topic = resource.get_cdmi_metadata().get('topic', '')
        script = resource.name
        script_type = magic.from_buffer(script_contents.getvalue())
        script_full_path = os.path.join(script_directory, script)
        script_path = os.path.dirname(script_full_path)
        logger.debug('Script "{0}" is apparently of type "{1}"'.format(script, script_type))

        if script_type in ('Python script, ASCII text executable', 'ASCII text'):
            logger.info('{1} script "{0}" for topic "{2}" at "{3}"'.format(script, 'create',
                                                                           trigger_topic, script_full_path))

            if not os.path.exists(script_path):
                os.makedirs(script_path)

            with open(script_full_path, 'w') as f:
                f.write(script_contents.getvalue())

            scripts[script] = {'topic': trigger_topic, 'path': script_full_path}


def init_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    client.connect('localhost', 1883, 60)

    return client


# noinspection PyUnusedLocal
def shutdown(_signo, _stack_frame):
    logger.info('Stopping MQTT...')
    mqtt_client.disconnect()

    logger.info('Dereticulating splines... Done!')

    sys.exit(0)

if __name__ == '__main__':
    arguments = docopt(__doc__, version='Listener v1.0')

    logger = log.init_log('listener')

    signal.signal(signal.SIGTERM, shutdown)

    cfg = get_config(None)
    initialise(keyspace=cfg.get('KEYSPACE', 'indigo'),
               hosts=cfg.get('CASSANDRA_HOSTS', ('127.0.0.1', )),
               repl_factor=cfg.get('REPLICATION_FACTOR', 1))

    if arguments['--verbose']:
        logger.setLevel(logging.DEBUG)
    elif arguments['--quiet']:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    script_directory_topic = '+/resource/{0}/#'.format(arguments['<script_collection>'])
    script_directory_topic = '/'.join(filter(None, script_directory_topic.split('/')))
    script_directory = arguments['<script_directory>']
    scan_script_collection(arguments['<script_collection>'])

    DEVNULL = open('/dev/null', 'w')

    mqtt_client = init_mqtt()
    mqtt_loop()
