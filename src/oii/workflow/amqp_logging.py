import logging
import pika
import sys
import re
import socket

DEFAULT_BROKER_URL='amqp://guest:guest@localhost:5672/%2f'

def declare_log_exchange(exchange,routing_key='',broker_url=DEFAULT_BROKER_URL):
    """Declare a "log exchange"
    A log exchange is a fanout exchange. Should be consumed with no_ack=True"""
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange,exchange_type='fanout')
    return channel, connection

class RabbitLogHandler(logging.Handler):
    def __init__(self,level=logging.NOTSET,exchange='logs',routing_key='',broker_url=DEFAULT_BROKER_URL):
        logging.Handler.__init__(self,level=level) # old-style superclass chaining
        self.broker_url = broker_url
        self.exchange = exchange
        self.routing_key = routing_key
        self.connection = None
        self.channel = None
        try:
            self.hostname = socket.gethostname()
        except:
            self.hostname = 'localhost'
    def emit(self,record):
        if self.connection is not None and self.connection.is_closed:
            self.connection = None
            self.channel = None
        try:
            if self.channel is None:
                self.channel, self.connection = declare_log_exchange(self.exchange, self.routing_key, self.broker_url)
            if self.channel is not None:
                formatted = '[%s] %s' % (self.hostname, self.format(record))
                self.channel.basic_publish(exchange=self.exchange,routing_key=self.routing_key,body=formatted)
        except pika.exceptions.ChannelClosed:
            self.channel = None # channel has likely died and will need to be reestablished
            self.connection = None
        except pika.exceptions.ConnectionClosed:
            self.channel = None # channel has likely died and will need to be reestablished
            self.connection = None
    def consume(self,out=sys.stdout,callback=None):
        ch, _ = declare_log_exchange(self.exchange,self.routing_key,self.broker_url)
        result = ch.queue_declare(exclusive=True)
        qname = result.method.queue
        ch.queue_bind(exchange=self.exchange,queue=qname)
        def printer(ch,m,p,b):
            out.write(b.rstrip()+'\n')
            out.flush()
        if callback is None:
            callback = printer
            ch.basic_consume(callback,queue=qname,no_ack=True)
        ch.start_consuming()

if __name__=='__main__':
    broker_url = sys.argv[1]
    try:
        exchange = sys.argv[2]
    except:
        exchange = 'logs'
    try:
        routing_key = sys.argv[3]
    except:
        routing_key = ''
    handler = RabbitLogHandler(exchange=exchange,routing_key=routing_key,broker_url=broker_url)
    handler.consume()
