from functools import partial
import logging
from typing import Callable, Any, Iterable
from collections import defaultdict

from kombu import Connection
from kombu.mixins import ConsumerMixin

from classic.components import component

from .handlers import MessageHandler, SimpleMessageHandler
from .scheme import BrokerScheme

logger = logging.getLogger(__file__)


AnyCallable = Callable[[Any], None]


@component
class KombuConsumer(ConsumerMixin):
    connection: Connection
    scheme: BrokerScheme

    def __attrs_post_init__(self):
        self._handlers = defaultdict(list)

    def _get_queues(self, queue_names: Iterable[str]):
        queues = []
        for name in queue_names:
            assert name in self.scheme.queues, \
                f'Queue with name {name} do not exists in broker scheme!'
            queues.append(self.scheme.queues[name])
        return queues

    def register_handler(self, handler: MessageHandler, *queue_names: str):
        queues = self._get_queues(queue_names)
        self._handlers[handler].extend(queues)

    def register_function(self,
                          function: AnyCallable,
                          *queue_names: str,
                          late_ack: bool = True):
        handler = SimpleMessageHandler(
            function=function, late_ack=late_ack,
        )
        queues = self._get_queues(queue_names)
        self._handlers[handler].extend(queues)

    def get_consumers(self, consumer_cls, channel):
        consumers = []
        for handler, queues in self._handlers.items():
            on_message = partial(self.on_message, handler=handler)
            c = consumer_cls(
                queues=queues,
                callbacks=[on_message],
            )
            consumers.append(c)
        return consumers

    @staticmethod
    def on_message(body, message, handler):
        try:
            logger.info(f'Trying to call {handler}')

            handler.handle(message, body)
        except Exception as error:
            logger.error(error)

    def run(self, *args, **kwargs):
        logger.info('Worker started')
        return super().run(*args, **kwargs)
