# EQueue
Elastic Queue is a library to bear a PubSub projects

### How to use it

Install via PIP

```shell
    $ pip install equeue
```

#### Producing

Using ipython

```python

    In [1]: from equeue.rabbit.publisher import Publisher

    In [2]: pub = Publisher(host='localhost', username='guest', password='guest', queue_name='t')

    In [3]: pub.put(message_dict={'id': 1})
    Out[3]: <promise@0x10ef18b78>

```

#### By poll

Using ipython

```python

    In [1]: from equeue.rabbit.subscriber import Subscriber

    In [2]: sub = Subscriber(host='localhost', username='guest', password='guest', queue_name='t')

    In [3]: msg = sub.get()

    In [4]: if msg:
       ...:     print(msg)
       ...:      
```

#### Consuming

Creating a main.py you'd see better.

```python

    from equeue.rabbit.subscriber import Subscriber

    def events_out(callback, message, delivery_tag):
        print(message)
        print(delivery_tag)
        callback.ack(delivery_tag)

    def main():

        sub = Subscriber(host='localhost', username='guest',
                password='guest', queue_name='t')
        
        sub.setup_consumer(callback=events_out)
        while True:
            sub.consume()
    if __name__ == '__main__':
        main()

```

Then

```shell
    $ python main.py
``` 

### Developing mode

Run tests

```shell
    $ py.test
```

