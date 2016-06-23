#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import os
import getopt
import importlib

import yaml
import logging

from .zmq import ZeroMQ

__version__ = '0.3.0';

def usage():
    '''Usage: debade-courier <zmq-address> -c <config-file> <-d>

    <zmq-address>: ZeroMQ address to listen, e.g. ipc:///path/to/ipc, tcp://0.0.0.0:3333
    <config-file>: path to config file in YAML format, e.g. /etc/debade/courier.yml
    '''
    print(usage.__doc__)


def main():

    logger = logging.getLogger('debade-courier')

    config_file = '/etc/debade-courier.yml'
    debug = False
    
    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], 'c:v', ['config=', 'verbose'])
    except getopt.GetoptError as e:
        # print help information and exit:
        usage()
        sys.exit(2)

    # get address
    address = args[0] if len(args) else 'tcp://127.0.0.1:3333'

    for option, argument in opts:

        if option in ('-c', '--config'):
            config_file = argument

        if option in('-v', '--verbose'):
            debug = True

    if not os.path.isfile(config_file):
        print('missing config file: {config_file}'.format(conf_file=config_file))
        sys.exit(2)
    else:
        with open(config_file) as f:
            config = yaml.load(f)

    if config.get('debug', False):
        debug = True

    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')

    if debug:
        logger.setLevel(logging.DEBUG)
        logger.info('Debug Mode')
    else:
        logger.setLevel(logging.INFO)

    servers_conf = config['servers']

    z = ZeroMQ(address=address, logger=logger)

    # waiting for message
    logger.info('Waiting for debade request at {address}'.format(address=address))

    mq = {}

    while True:
        try:
            o = z.recv()
            if 'queue' not in o:
                continue

            q = o.get('queue')
            if q not in mq:
                if q not in servers_conf:
                    logger.debug('unknown queue:[{queue}]! drop it'.format(queue=q))
                    continue
                server_conf = servers_conf[q]
                driver = server_conf.get('driver', 'rabbitmq')
                mq[q] = importlib.import_module('.mq.' + driver, __name__).Queue(name=q, 
                    logger=logger, conf=server_conf)

            if q in mq:
                mq[q].push(routing_key=o.get('routing', ''), 
                    data=o.get('data', {}))
        except KeyboardInterrupt:
            break

if __name__ == '__main__':
    main()
