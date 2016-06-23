# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pika
import json

class Queue(object):

    def __init__(self, name, logger, conf):
        self.name = name
        self.logger = logger
        self.conn_params = pika.ConnectionParameters(
            host=conf.get('host', '127.0.0.1'), 
            port=conf.get('port', 5672),
            credentials=pika.credentials.PlainCredentials(
                username=conf.get('username'), password=conf.get('password')))
        self.exchange = conf.get('exchange', 'default')
        self.type = conf.get('type', 'fanout')
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
            self.logger.error('MQ[{name}] connect error: {error}'.format(name=self.name, error=str(e)))
        except Exception:
            pass

    def flush(self):
        while self.msg_queue:
            msg = self.msg_queue[0]
            routing_key = msg['r']
            body = json.dumps(msg['d'], ensure_ascii=False)
            try:
                self.ch.basic_publish(exchange=self.exchange,
                    routing_key=routing_key,
                    body=body)

                self.logger.debug('MQ[{name}] <= {body}'.format(name=self.name, body=body))

                 # 只有在一切正确没有异常的情况下再从消息队列中清除该消息
                self.msg_queue.pop(0)
            except Exception as e:
                self.logger.error('MQ[{name}] push error: {body}'.format(name=self.name, body=e))
                msg['f'] += 1
                # 发送失败三次就洗洗睡吧
                msg['f'] < 3 or self.msg_queue.pop(0)
                self.connect()
        
    def push(self, routing_key, data):
        # 首先先将消息放入队列, 然后再flush
        self.msg_queue.append({'r':routing_key, 'd':data, 'f':0})
        self.flush()


