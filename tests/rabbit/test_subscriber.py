from datetime import datetime
import json
import unittest

from amqp import Message, AMQPError, ConnectionError
from mock import MagicMock, patch, call, Mock, ANY

from equeue.rabbit.subscriber import Subscriber


class SubscriberTest(unittest.TestCase):
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

        self.subscriber = Subscriber('localhost', 'username', 'pwd',
                                            exchange='default_exchange',
                                            queue_name='default_queue')

    def tearDown(self):
        self.connection_cls_patcher.stop()

    def test_get_uses_default_queue_if_not_supplied(self):
        self.subscriber.get()
        self.assertEqual(self.channel_mock.basic_get.call_args_list,
                         [call(queue='default_queue')])

    def test_get_none_if_block_is_false_and_queue_is_empty(self):
        self.channel_mock.basic_get.return_value = None
        rv = self.subscriber.get(block=False)
        self.assertEqual(rv, (None, None))

    def test_get_sleeps_and_tries_again_until_queue_is_not_empty(self):
        empty_rv = None
        self.channel_mock.basic_get.side_effect = [empty_rv, empty_rv, self.message]
        with patch('time.sleep') as sleep,\
                patch('equeue.rabbit.queue.RabbitQueue._parse_message') as parse_message_mock:
            message = self.subscriber.get(queue_name='queue_name')

            self.assertEqual(message, parse_message_mock.return_value)
            self.assertEqual(parse_message_mock.call_args_list,
                             [call(self.message)])
            self.assertEqual(self.channel_mock.basic_get.call_args_list,
                             [call(queue='queue_name'),
                              call(queue='queue_name'),
                              call(queue='queue_name')])
            self.assertEqual(sleep.call_count, 2)

    def test_get_error_if_default_queue_does_not_exist(self):
        self.connection_cls_mock.return_value.channel.side_effect = ConnectionError
        with self.assertRaises(ConnectionError):
            self.subscriber.get()


