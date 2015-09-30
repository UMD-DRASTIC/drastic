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
  listener.py <script_directory> [--quiet | --verbose]
  listener.py -h | --help

Options:
  -h, --help  Show this message.
  --verbose   Increase logging output to DEBUG level.
  --quiet     Decrease logging output to WARNING level.

"""

import logging
import json

from docopt import docopt
import paho.mqtt.client as mqtt

from indigo import get_config
import log
from models import initialise

scripts = dict()


def on_connect(client, userdata, flags, rc):
    logger.info('Connected to MQTT broker with result code {0}'.format(str(rc)))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe('#')
    logger.info('Subscribing to all topics')
    logger.info('Listening on "{0}" for scripts'.format(script_directory_topic))


def on_message(client, userdata, msg):
    if mqtt.topic_matches_sub(script_directory_topic, msg.topic):
        path = msg.topic.split('/')
        operation = path[0]
        script = '/'.join(path[3:])
        payload = json.loads(msg.payload)

        try:
            trigger_topic = payload['metadata']['topic']
        except KeyError:
            logger.warning('Script "{0}" does not have a trigger topic. Ignoring.'.format(script))
            logger.debug('Payload: {}'.format(json.dumps(payload)))
            return

        if operation is 'delete':
            del scripts[script]
        else:
            scripts[script] = trigger_topic

        logger.info('{1} script "{0}" for topic "{2}"'.format(script, operation, trigger_topic))
        logger.debug('Payload: {}'.format(json.dumps(payload)))
    else:
        useful_topic = False

        for script, sub in scripts.iteritems():
            if mqtt.topic_matches_sub(sub, msg.topic):
                useful_topic = True
                logger.info('execute script "{0}" for topic "{1}"'.format(script, msg.topic))

        if not useful_topic:
            logger.debug('Topic: {}'.format(msg.topic))


def scan_script_collection(directory):
    logging.info('Scanning "{0}" for scripts'.format(directory))


def init_mqtt():
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect('localhost', 1883, 60)
    client.loop_forever()


if __name__ == '__main__':
    arguments = docopt(__doc__, version='Listener v1.0')

    logger = log.init_log('listener')

    cfg = get_config(None)
    initialise(cfg.get('KEYSPACE', 'indigo'))

    if arguments['--verbose']:
        logger.setLevel(logging.DEBUG)
    elif arguments['--quiet']:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    script_directory_topic = '+/resource/{0}/#'.format(arguments['<script_directory>'])
    script_directory_topic = '/'.join(filter(None, script_directory_topic.split('/')))
    script_directory = '/' + '/'.join(script_directory_topic.split('/')[2:-1])
    scan_script_collection(script_directory)
    init_mqtt()
