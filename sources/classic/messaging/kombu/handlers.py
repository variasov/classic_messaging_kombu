from abc import ABC, abstractmethod
from typing import Dict, Any, Callable

from kombu import Message

from classic.components import component


MessageBody = Dict[str, Any]


@component
class MessageHandler(ABC):

    @abstractmethod
    def handle(self, message: Message, body: MessageBody):
        pass


@component
class SimpleMessageHandler(MessageHandler):
    function: Callable[[Any], Any]
    late_ack: bool = True

    def handle(self, message: Message, body: MessageBody):
        if not self.late_ack:
            message.ack()

        try:
            self.function(**body)
        except Exception as exc:
            if self.late_ack:
                message.reject()
            raise exc
        else:
            if self.late_ack:
                message.ack()
