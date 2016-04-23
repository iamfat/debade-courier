#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import os
import getopt

import pika
import zmq
import yaml
import logging
import json

__version__ = '0.3.0';

logger = logging.getLogger('debade-courier')

class ZeroMQ(object):
    
    def __init__(self, address):
        self.address = address
        try:
            self.ctx = zmq.Context()
            self.sock = self.ctx.socket(zmq.PULL)
            self.sock.bind(address)
        except zmq.error.ZMQError as e:
            logger.error('failed to bind {address}: {error}'.format(addr=self.address, error=str(e)))
            sys.exit(2)

    def recv(self):
        o = self.sock.recv_json()
        logger.debug('0MQ => {body}'.format(body=json.dumps(o, ensure_ascii=False)))
        return o


class Rabbit(object):

    def __init__(self, name, host, port, username, password, exchange, type):
        self.name = name
        self.conn_params = pika.ConnectionParameters(
            host=host, port=port,
            credentials=pika.credentials.PlainCredentials(
                username=username, password=password))
        self.exchange = exchange
        self.type = type
        self.msg_queue = []
        self.connect()

    def connect(self):
        try:
            self.conn = pika.BlockingConnection(self.conn_params)
            self.ch = self.conn.channel()
            self.ch.exchange_declare(
                exchange=self.exchange, 
                type=self.type, 
                durable=False, 
                auto_delete=True )
            # connect 成功之后, 先看看有没有要继续发送的消息
            self.flush()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error('MQ[{name}] connect error: {error}'.format(name=self.name, error=str(e)))
        except Exception:
            pass

    def flush(self):
        while self.msg_queue:
            msg = self.msg_queue[0]
            routing_key = msg['r']
            body = json.dumps(msg['b'], ensure_ascii=False)
            try:
                self.ch.basic_publish(exchange=self.exchange,
                    routing_key=routing_key,
                    body=body)

                logger.debug('MQ[{name}] <= {body}'.format(name=self.name, body=body))

                 # 只有在一切正确没有异常的情况下再从消息队列中清除该消息
                self.msg_queue.pop(0)
            except Exception as e:
                logger.error('MQ[{name}] publish error: {body}'.format(name=self.name, body=e))
                msg['f'] += 1
                # 发送失败三次就洗洗睡吧
                msg['f'] < 3 or self.msg_queue.pop(0)
                self.connect()
        
    def publish(self, routing_key, body):
        # 首先先将消息放入队列, 然后再flush
        self.msg_queue.append({'r':routing_key, 'b':body, 'f':0})
        self.flush()


def usage():
    '''Usage: debade-courier <zmq-address> -c <config-file> <-d>

    <zmq-address>: ZeroMQ address to listen, e.g. ipc:///path/to/ipc, tcp://0.0.0.0:3333
    <config-file>: path to config file in YAML format, e.g. /etc/debade/courier.yml
    '''
    print(usage.__doc__)


def main():

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

    z = ZeroMQ(address=address)

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
                mq[q] = Rabbit(name=q, 
                            host=server_conf.get('host', '127.0.0.1'), 
                            port=server_conf.get('port'), 
                            username=server_conf.get('username'), 
                            password=server_conf.get('password'), 
                            exchange=server_conf.get('exchange', 'default'), 
                            type=server_conf.get('type', 'fanout'))
        
            mq[q].publish(routing_key=o.get('routing', ''), body=o.get('data', {}))
        except KeyboardInterrupt:
            break

if __name__ == '__main__':
    main()
