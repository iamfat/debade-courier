# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import zmq
import json

class Queue(object):

    def __init__(self, name, logger, conf):
        self.name = name
        self.logger = logger
        host = conf.get('host', '127.0.0.1')
        port = conf.get('port', '3334')
        self.address = 'tcp://' + host + ':' + str(port)
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.PUB)
        self.sock.bind(self.address)

    def push(self, routing_key, data):
        # 首先先将消息放入队列, 然后再flush
        try:
            body = json.dumps(data, ensure_ascii=False)
            self.logger.debug('MQ[{name}] <= {body}'.format(name=self.name, body=body))
            self.sock.send_string(body)
        except Exception as e:
            self.logger.error('MQ[{name}] push error: {body}'.format(name=self.name, body=e))

