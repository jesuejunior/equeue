from datetime import datetime
import json
import unittest

from amqp import Message, AMQPError, ConnectionError
from mock import MagicMock, patch, call, Mock, ANY

from rabbit.queue import (RabbitQueue, MAX_TRIES, SerializationError, META_FIELD)

MODULE = 'rabbit.queue.'


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

        self.external_queue = RabbitQueue('localhost', 'username', 'pwd',
                                            exchange='default_exchange',
                                            queue_name='default_queue')

    def tearDown(self):
        self.connection_cls_patcher.stop()

    def test_connect_calls_close_before_creating_a_new_connection(self):
        with patch(MODULE + 'RabbitQueue.close') as close_mock:
            self.external_queue._connect()
            self.assertEqual(close_mock.call_count, 1)

    def test_connect_ignores_close_errors(self):
        with patch.object(self.external_queue, 'close', side_effect=[Exception]) as mock_close:
            self.external_queue._connect()
            self.assertRaises
            self.assertEqual(mock_close.call_count, 1)

    def test_connect_subscribes_if_subscription_is_set(self):
        with patch.object(self.external_queue, 'close'), \
             patch.object(self.external_queue, '_subscribe') as mock_subscribe:
            self.external_queue.subscription = 'routing_key'
            self.external_queue._connect()

            self.assertEqual(mock_subscribe.call_count, 1)

    def test__try_connects_if_disconnected(self):
        self.channel_mock.method.return_value = 'rv'
        rv = self.external_queue._try('method', arg='value')

        self.assertEqual(self.connection_cls_mock.call_count, 1)
        self.assertEqual(self.channel_mock.method.call_args_list,
                         [call(arg='value')])
        self.assertEqual(rv, 'rv')

    def test__try_tries_up_to_max_tries(self):
        self.channel_mock.method.side_effect = [AMQPError, AMQPError, 'rv']
        rv = self.external_queue._try('method')

        self.assertEqual(self.channel_mock.method.call_count, MAX_TRIES)
        self.assertEqual(rv, 'rv')

    def test__try_doesnt_try_more_than_max_tries(self):
        self.channel_mock.method.side_effect = [AMQPError, AMQPError, AMQPError, 'rv']
        self.assertRaises(ConnectionError, self.external_queue._try, 'method')

    def test_parse_message_deserializes_the_message_body_and_sets_meta_field(self):
        message, ack = self.external_queue._parse_message(self.message)
        self.assertEqual(message[META_FIELD], {})
        self.assertEqual(ack, 'delivery_tag')

    def test_malformed_message_should_ack(self):
        queue = self.external_queue
        self.message.body = "{'foo': True}"
        with patch.object(queue, 'ack') as mock_ack:
            result = queue._parse_message(self.message)
            self.assertIsNone(result)
            self.assertEqual(mock_ack.call_args_list, 
                    [call(self.message.delivery_info['delivery_tag'])])

    def test_setup_consumer(self):
        callback = Mock()
        self.external_queue.connection = Mock()
        self.external_queue.channel = Mock()
        with patch(MODULE + 'RabbitQueue._try') as try_mock:
            self.external_queue.setup_consumer(callback, ['queue_1', 'queue_2'])

            self.assertEqual(try_mock.call_args_list,
                             [call('basic_qos', prefetch_size=0,
                                   prefetch_count=1, a_global=False)])
            self.assertEqual(self.external_queue.channel.basic_consume.call_args_list,
                             [call('queue_1', callback=ANY),
                              call('queue_2', callback=ANY)])

    def test_consume(self):
        self.external_queue.connection = Mock()
        self.external_queue.consume()
        self.assertEqual(self.external_queue.connection.drain_events.call_count, 1)

    def test_ack_ignores_connection_errors(self):
        self.external_queue.channel = self.channel_mock
        self.channel_mock.basic_ack.side_effect = AMQPError
        self.external_queue.ack('delivery_tag')

    def test_reject_ignores_connection_errors(self):
        self.external_queue.channel = self.channel_mock
        self.channel_mock.basic_reject.side_effect = AMQPError
        self.external_queue.reject('delivery_tag')

    def test_does_not_crash_on_context_management(self):
        queue = self.external_queue
        with queue as q:
            self.assertEqual(q, queue)
        # Do nothing to force close without connection

    def test_subscribe_declares_queue(self):
        with patch.object(self.external_queue, 'channel') as mock_channel:
            self.external_queue._subscribe()

            self.assertEqual(mock_channel.queue_declare.call_args_list, [call(queue=self.external_queue.default_queue_name, durable=False, exclusive=True, auto_delete=True)])

    def test_subscribe_binds_to_queue(self):
        with patch.object(self.external_queue, 'channel') as mock_channel:
            self.external_queue._subscribe()

            self.assertEqual(mock_channel.queue_bind.call_args_list, [call(exchange='notifications', queue=self.external_queue.default_queue_name, routing_key=self.external_queue.subscription)])

    def test_subscribe_sets_subscription(self):
        with patch.object(self.external_queue, '_connect'):
            self.external_queue.subscribe('routing_key')

            self.assertEqual('routing_key', self.external_queue.subscription)

    def test_subscribe_connects_to_server(self):
        with patch.object(self.external_queue, '_connect') as mock_connect:
            self.external_queue.subscribe('routing_key')

            self.assertEqual(mock_connect.call_count, 1)

    def test_message_callback_parses_message(self):
        def callback(message=None, delivery_tag=None):
            return message

        def stub_basic_consume(queue_name, callback):
            callback(self.message)

        mock_channel = Mock()
        mock_channel.basic_consume = stub_basic_consume

        with patch.object(self.external_queue, '_try'), \
             patch.object(self.external_queue, '_parse_message', \
             return_value=self.message) as mock_parse_message:
            self.external_queue.channel = mock_channel
            self.external_queue.setup_consumer(callback, ['queue_1'])

            self.assertEqual(mock_parse_message.call_args_list, [call(self.message)])


class ExceptionsTest(unittest.TestCase):
    def test_serializationerror_message(self):
        excp = Exception('Some random error')

        serialization_error = SerializationError(excp)

        self.assertEqual('{}'.format(serialization_error), 'Some random error')
