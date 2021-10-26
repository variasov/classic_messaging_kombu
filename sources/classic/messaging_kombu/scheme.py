from backports.cached_property import cached_property
from kombu import Queue, Connection


class BrokerScheme:

    def __init__(self, *queues: Queue):
        self._queues = queues
        self._exchanges = [
            queue.exchange
            for queue in self._queues
        ]

    @cached_property
    def exchanges(self):
        return {exchange.name: exchange for exchange in self._exchanges}

    @cached_property
    def queues(self):
        return {queue.name: queue for queue in self._queues}

    def declare(self, connection: Connection):
        with connection.channel() as channel:

            for exchange in self._exchanges:
                exchange.declare(channel=channel)

            for queue in self._queues:
                queue.declare(channel=channel)
