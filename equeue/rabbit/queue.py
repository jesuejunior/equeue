from datetime import datetime
import time
import uuid

import amqp
from amqp import Message, AMQPError, ConnectionError as AMQPConnectionError
import simplejson as json

MAX_TRIES = 3
META_FIELD = "_meta"
# mainly, this class I took from https://github.com/sievetech/hived/blob/master/hived/queue.py
# and adapted for my necessity. 


class ConnectionError(AMQPConnectionError):
    def __str__(self):
        return '%s' % self.message


class SerializationError(Exception):
    def __init__(self, exc, body=None):
        super(Exception, self).__init__(*exc.args)
        self.exc = exc
        self.body = body

    def __repr__(self):
        return '%s: %s' % (self.exc, repr(self.body))


class RabbitQueue(object):
    """
    For getting messages from the queue, see get() in the Subscriber class.
    For publishing, see put() in the Publisher class.
    The connection is lazy, i.e. it only happens on the first get() / put().
    """
    
    def __init__(self, host='localhost', username='guest', password='guest',
            virtual_host='/', exchange=None, queue_name=None, queue_heartbeat=None):
        self.default_exchange = exchange
        self.default_queue_name = queue_name
        self.channel = None
        self.subscription = None
        self.connection = None

        self.connection_parameters = {
            'host': host,
            'userid': username,
            'password': password,
            'virtual_host': virtual_host,
            'heartbeat': queue_heartbeat
        }

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False

    def _connect(self):
        try:
            self.close()
        except Exception:
            pass
        self.connection = amqp.Connection(**self.connection_parameters)
        self.connection.connect()
        self.channel = self.connection.channel()
        if self.subscription:
            self._subscribe()

    def close(self):
        if self.connection is not None:
            self.connection.close()

    def _try(self, method, _tries=1, **kwargs):
        if self.channel is None:
            self._connect()

        try:
            # import ipdb; ipdb.set_trace()
            return getattr(self.channel, method)(**kwargs)
        except (AMQPError, IOError) as e:
            if _tries < MAX_TRIES:
                self._connect()
                return self._try(method, _tries + 1, **kwargs)
            else:
                raise ConnectionError(e)

    def _subscribe(self):
        self.default_queue_name = '%s_%s' % (self.subscription, uuid.uuid4())
        self.channel.queue_declare(queue=self.default_queue_name,
                                   durable=False,
                                   exclusive=True,
                                   auto_delete=True)
        self.channel.queue_bind(exchange='notifies',
                                queue=self.default_queue_name,
                                routing_key=self.subscription)

    def subscribe(self, routing_key):
        self.subscription = routing_key
        self._connect()

    def _parse_message(self, message):
        body = message.body
        delivery_tag = message.delivery_info['delivery_tag']
        try:
            message_dict = json.loads(body)
            message_dict.setdefault(META_FIELD, {})
        except Exception:
            self.ack(delivery_tag)
            return
        return message_dict, delivery_tag

    def setup_consumer(self, callback, queue_names=None):
        # import ipdb; ipdb.set_trace()
        def message_callback(message):
            parsed = self._parse_message(message)
            if parsed:
                callback(self, *parsed)

        self._try('basic_qos', prefetch_size=0, prefetch_count=1, a_global=False)
        queue_names = queue_names or [self.default_queue_name]
        for queue_name in queue_names:
            self.channel.basic_consume(queue_name, callback=message_callback)
    
    def consume(self):
        self.connection.drain_events()

    def ack(self, delivery_tag):
        """
        Acks a message from the queue.
        delivery_tag: second value on the tuple returned from get().
        """
        try:
            self.channel.basic_ack(delivery_tag)
        except AMQPError:
            # There's nothing we can do, we can't ack the message in
            # a different channel than the one we got it from
            pass

    def reject(self, delivery_tag):
        """
        Rejects a message from the queue, i.e. returns it to the top of the queue.
        delivery_tag: second value on the tuple returned from get().
        """
        try:
            self.channel.basic_reject(delivery_tag, requeue=True)
        except AMQPError:
            pass  # It's out of our hands already
