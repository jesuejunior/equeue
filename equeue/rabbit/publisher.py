# encondign: utf-8
import simplejson as json
from amqp import Message
from equeue.rabbit.queue import RabbitQueue, SerializationError


class Publisher(RabbitQueue):

    def put(self, message_dict=None, routing_key='', exchange=None, body=None, priority=0):
        """
        Publishes a message to the queue.
        message_dict: the json-serializable object that will be published
            when body is None
        routing_key: the routing key for the message.
        exchange: the exchange to which the message will be published.
        body: The message to be published. If none, message_dict is published.
        
        It also works as a context manager:
        with Publisher(**options) as queue:
            for msg in msgs:
                queue.put(msg)
        """
        if exchange is None:
            exchange = self.default_exchange or ''
        if body is None:
            try:
                body = json.dumps(message_dict)
            except Exception as e:
                raise SerializationError(e)

        message = Message(body,
                          delivery_mode=2,
                          content_type='application/json',
                          priority=priority
                          )
        return self._try('basic_publish',
                         msg=message,
                         exchange=exchange,
                         routing_key=routing_key)

    def flush(self):
        self.put("flush", routing_key="/dev/null")
