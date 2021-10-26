# Classic Messaging Kombu

This package provides implementation of interfaces in classic-messaging and 
base for consumers and messages handling.

Usage with publishing:

```python
from classic.components import component
from classic.messaging import Message, Publisher
from classic.messaging_kombu import BrokerScheme, KombuPublisher
from kombu import Exchange, Queue, Connection


@component
class SomeService:
    publisher: Publisher

    def do_some_work(self):
        message = Message('some', 'Some very useful info')
        self.publisher.publish(message)


broker_scheme = BrokerScheme(
    Queue('queue1', Exchange('some')),
)
        
connection = Connection('amqp://localhost:5672/')
publisher = KombuPublisher(
    connection=connection,
    scheme=broker_scheme
)

service = SomeService(publisher=publisher)

service.do_some_work()

```


Usage with consuming:
```python
from classic.messaging_kombu import BrokerScheme, KombuConsumer
from kombu import Exchange, Queue, Connection


class SomeSerice:
    def handle_message(self, message):
        print(message)


broker_scheme = BrokerScheme(
    Queue('queue1', Exchange('some')),
)

connection = Connection('amqp://localhost:5672/')

consumer = KombuConsumer(
    connection=connection,
    scheme=broker_scheme,
)

service = SomeSerice()
consumer.register_function(service.handle_message, 'queue1')

```