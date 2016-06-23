# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import sys
import zmq
import json

class ZeroMQ(object):
    
    def __init__(self, address, logger):
        self.address = address
        self.logger = logger
        try:
            self.ctx = zmq.Context()
            self.sock = self.ctx.socket(zmq.PULL)
            self.sock.bind(address)
        except zmq.error.ZMQError as e:
            self.logger.error('failed to bind {address}: {error}'.format(addr=self.address, error=str(e)))
            sys.exit(2)

    def recv(self):
        o = self.sock.recv_json()
        self.logger.debug('0MQ => {body}'.format(
            body=json.dumps(o, ensure_ascii=False)))
        return o


