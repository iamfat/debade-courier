# -*- coding: utf-8 -*-

import sys
import os
import getopt

import pika
import zmq
import yaml
import logging
import json

logger = logging.getLogger('debade-courier')

class ZeroMQ:
    
    def __init__(self, addr):
        self.addr = addr
        try:
            self.ctx = zmq.Context()
            self.sock = self.ctx.socket(zmq.PULL)
            self.sock.bind(addr)
        except zmq.error.ZMQError as e:
            logger.error("failed to bind %s: %s" % (self.addr, str(e)))
            sys.exit(2)
    
    def recv(self):
        o = self.sock.recv_json()
        logger.debug("0MQ => %s" % json.dumps(o))
        return o

class Rabbit:
    
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
            logger.error("MQ[%s] connect error: %s" % (self.name, str(e)))
        except:
            return
            
    def __init__(self, name, host, port, username, password, 
                exchange, type):
        self.name = name
        self.conn_params = pika.ConnectionParameters(
            host=host, port=port, 
            credentials=pika.credentials.PlainCredentials(
                username=username, password=password))
        self.exchange = exchange
        self.type = type
        self.msg_queue = []
        self.connect()
    
    def flush(self):
        while self.msg_queue:
            msg = self.msg_queue[0]
            routing_key = msg['r']
            body = json.dumps(msg['b'])
            try:
                self.ch.basic_publish(exchange=self.exchange, 
                    routing_key=routing_key, body=body)
                logger.debug("MQ[%s] <= %r" % (self.name, body))
                 # 只有在一切正确没有异常的情况下再从消息队列中清除该消息
                self.msg_queue.pop(0)
            except Exception as e:
                logger.error("MQ[%s] publish error: %s" % (self.name, str(e)))
                msg['f'] += 1
                # 发送失败三次就洗洗睡吧
                msg['f'] < 3 or self.msg_queue.pop(0)
                self.connect()
        
    def publish(self, routing_key, body):
        # 首先先将消息放入队列, 然后再flush
        self.msg_queue.append({'r':routing_key, 'b':body, 'f':0})
        self.flush()

def usage():
    print "Usage: debade-courier -c <config-file> <zmq-address>\n"
    print "    <config-file>: path to config file in YAML format, e.g. /etc/debade/courier.yml"
    print "    <zmq-address>: ZeroMQ address to listen, e.g. ipc:///path/to/ipc, tcp://0.0.0.0:3333"

def main():

    # debade-courier -c /etc/debade-courier.yml <>
    
    conf_file = '/etc/debade-courier.yml'
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:v", ["config="])
    except getopt.GetoptError as e:
        # print help information and exit:
        usage()
        sys.exit(2)

    if len(args) > 0:
        addr = args[0]
    else:
        addr = "tcp://127.0.0.1:3333"

    verbose = 0
    for o, a in opts:
        if o == "-c":
            conf_file = a
        elif o == "-v":
            verbose+=1
        else:
            assert False, "unhandled option"
    
    if not os.path.isfile(conf_file):    
        print("missing config file: %s" % conf_file)
        sys.exit(2)
        
    f = open(conf_file)
    conf = yaml.safe_load(f)
    f.close()

    if 'debug' in conf and conf['debug']==True:
        verbose = 3

    ch = logging.StreamHandler()
    if verbose>0 :
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)
    if verbose>0 :
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    servers_conf = conf['servers']

    z = ZeroMQ(addr=addr)

    # waiting for message
    logger.info("Waiting for debade request at %s" % addr)

    mq = {}

    while True:
        o = z.recv()
        if 'queue' not in o:
            continue

        q = o.get('queue')
        if q not in mq :
            if q not in servers_conf:
                logger.debug("unknown queue:%s! drop it" % q) 
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


if __name__ == "__main__":
    main()