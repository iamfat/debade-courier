# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import json
import beanstalk

class Queue(object):

    msg_queue = []

    def __init__(self, name, logger, conf):
        self.name = name
        self.logger = logger
        self.host = conf.get('host', '127.0.0.1')
        self.port = conf.get('port', 11300)
        self.tube = conf.get('tube', 'default')
        self.connect()

    def connect(self):
        try:
            self.conn = beanstalk.Connection(host=self.host, port=self.port)
            self.conn.use(self.tube)
            self.flush()
        except Exception as e:
            pass

    def flush(self):
        while self.msg_queue:
            msg = self.msg_queue[0]
            routing_key = msg['r']
            body = json.dumps(msg['b'], ensure_ascii=False)
            try:
                self.conn.put(body.encode('utf8'))
                self.logger.debug('MQ[{name}] <= {body}'.format(name=self.name, body=body))

                 # 只有在一切正确没有异常的情况下再从消息队列中清除该消息
                self.msg_queue.pop(0)
            except Exception as e:
                self.logger.error('MQ[{name}] push error: {body}'.format(name=self.name, body=e))
                msg['f'] += 1
                # 发送失败三次就洗洗睡吧
                msg['f'] < 3 or self.msg_queue.pop(0)
                self.connect()
        
    def push(self, routing_key, body):
        # 首先先将消息放入队列, 然后再flush
        self.msg_queue.append({'r':routing_key, 'b':body, 'f':0})
        self.flush()


