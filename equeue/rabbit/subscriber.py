# enconding: utf-8
import time
from rabbit.queue import RabbitQueue


class Subscriber(RabbitQueue):

    def get(self, queue_name=None, block=True):
        """
        Gets messages from the queue.
        queue_name: optional, defaults to the one passed on __init__().
        block: boolean. If block is True (default), get() will not return until
            a message is acquired from the queue.

        Returns a tuple (message, delivery_tag) when a message is read, where
        message is a deserialized json and delivery_tag is a parameter used
        for on ack() and reject() methods. If block is False and there's no
        message on the queue, returns (None, None).
        """
        while True:
            message = self._try('basic_get', queue=queue_name or self.default_queue_name)
            if message:
                parsed = self._parse_message(message)
                if parsed:
                    return parsed

            if block:
                time.sleep(.5)
            else:
                return None, None

