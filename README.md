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
Message have 2 arguments - target and body. Target is a str with destination. 
In simple case it is an exchange name, in complex case - entry in mapping.

Body can be any serializable object.


Customization of target:
```python
from classic.messaging import Message
from classic.messaging_kombu import BrokerScheme, KombuPublisher
from kombu import Exchange, Queue, Connection


targets = {
    'FIRST': {
        'exchange': 'exchange1',
    },
    'SECOND': {
        'exchange': 'exchange2',
        'timeout': 10,
    }
}

broker_scheme = BrokerScheme(
    Queue('queue1', Exchange('exchange1')),
    Queue('queue2', Exchange('exchange2')),
)
        
connection = Connection('amqp://localhost:5672/')
publisher = KombuPublisher(
    connection=connection,
    scheme=broker_scheme,
    messages_params=targets,
)

publisher = KombuPublisher(
    connection=connection,
    scheme=broker_scheme
)

publisher.publish(
    Message('FIRST', 'Hello'),  # Will be published to exchange1 and queue1
    Message('SECOND', 'By'),  # Will be published to exchange2, queue2 and timeout=10
)
```

Usage with consuming:
```python
from classic.messaging_kombu import BrokerScheme, KombuConsumer, MessageHandler
from kombu import Exchange, Queue, Connection


class SomeSerice:
    def handle_message(self, message):
        print(message)

        
class CustomHandler(MessageHandler):
    
    def handle(self, message, body):
        print(body)
        
        message.ack()


broker_scheme = BrokerScheme(
    Queue('queue1', Exchange('exchange1')),
    Queue('queue2', Exchange('exchange2')),
)

connection = Connection('amqp://localhost:5672/')

consumer = KombuConsumer(
    connection=connection,
    scheme=broker_scheme,
)

service = SomeSerice()
handler = CustomHandler()

consumer.register_function(service.handle_message, 'queue1')
consumer.register_handler(handler, 'queue2')

```