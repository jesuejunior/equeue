#!/usr/bin/env python
import threading, logging, time
import json
from rabbit.publisher import Publisher
from rabbit.subscriber import Subscriber

def events_out(callback, message, delivery_tag):
    print('This a full message', message)
    print('This is a deliver tag', delivery_tag)
    callback.ack(delivery_tag)


class Producer(threading.Thread):
    # daemon = True

    def run(self):
        pub = Publisher(host='localhost', username='guest',
            password='guest', queue_name='teste')
        while True:
            for i in xrange(10000):
                logging.info('producer')
                pub.put(body=json.dumps({'id': i}), routing_key='t')
            time.sleep(60)


class Consumer(threading.Thread):
    # daemon = True

    def run(self):
        sub = Subscriber(host='localhost', username='everest',
                password='everest', queue_name='t')
    
        sub.setup_consumer(callback=events_out)
        
        while True:
            # time.sleep(0.01)
            logging.info('consumer')
            sub.consume()

def main():
    threads = [
        Consumer(),
        Producer()
    ]

    for t in threads:
        t.start()
    # time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )
    main()
