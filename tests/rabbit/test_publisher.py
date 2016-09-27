from datetime import datetime
import json
import unittest

from amqp import Message, AMQPError, ConnectionError
from mock import MagicMock, patch, call, Mock, ANY

from rabbit.queue import SerializationError
from rabbit.publisher import Publisher


class RabbitQueueTest(unittest.TestCase):
    def setUp(self):
        _delivery_info = {'delivery_tag': 'delivery_tag'}

        self.message = MagicMock()
        self.message.body = json.dumps({'id': 123})
        self.message.delivery_info = _delivery_info

        self.channel_mock = MagicMock()
        self.channel_mock.basic_get.return_value = self.message

        self.connection = MagicMock()
        self.connection.channel.return_value = self.channel_mock

        self.connection_cls_patcher = patch('amqp.Connection',
                                                 return_value=self.connection)
        self.connection_cls_mock = self.connection_cls_patcher.start()

        self.publisher = Publisher('localhost', 'username', 'pwd',
                                            exchange='default_exchange',
                                            queue_name='default_queue')

    def tearDown(self):
        self.connection_cls_patcher.stop()

    def test_put_default_exchange_if_not_supplied(self):
        amqp_msg = Message('body', delivery_mode=2, content_type='application/json', priority=0)
        self.publisher.put(body='body')
        self.assertEqual(self.channel_mock.basic_publish.call_args_list,
                         [call(msg=amqp_msg,
                               exchange='default_exchange',
                               routing_key='')])

    def test_put_serializes_message_if_necessary(self):
        message = {'key': 'value'}
        with patch('rabbit.publisher.Publisher') as MockPublisher:
            self.publisher.put(message_dict=message)

        self.assertEqual(MockPublisher.call_args_list,
                         [call(json.dumps(message), delivery_mode=2,
                               content_type='application/json', priority=0)])

    def test_put_raises_serialization_error_if_cant_be_serialized_to_json(self):
        self.assertRaises(SerializationError, self.publisher.put, message_dict=ValueError)


