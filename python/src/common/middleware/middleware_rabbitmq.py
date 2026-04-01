import pika
import random
import string
from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None
        self.consumer_tag = None

        try:
            parameters = pika.ConnectionParameters(host=self.host)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name)
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        def _consume_callback(channel, method, properties, body):
            delivery_tag = method.delivery_tag

            def ack():
                channel.basic_ack(delivery_tag=delivery_tag)

            def nack():
                channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_qos(prefetch_count=1)
            self.consumer_tag = self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=_consume_callback,
                auto_ack=False,
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc
        finally:
            self.consumer_tag = None

    def stop_consuming(self):
        try:
            if (
                self.channel is not None
                and self.channel.is_open
                and self.consumer_tag is not None
            ):
                self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc

    def close(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareCloseError() from exc

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.queue_name = None
        self.connection = None
        self.channel = None
        self.consumer_tag = None

        try:
            parameters = pika.ConnectionParameters(host=self.host)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.exchange_declare(
                exchange=self.exchange_name,
                exchange_type="direct",
            )

            queue_result = self.channel.queue_declare(
                queue="",
                exclusive=True,
                auto_delete=True,
            )
            self.queue_name = queue_result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(
                    exchange=self.exchange_name,
                    queue=self.queue_name,
                    routing_key=routing_key,
                )
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc

    def start_consuming(self, on_message_callback):
        raise NotImplementedError

    def stop_consuming(self):
        return None

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(
                    exchange=self.exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except pika.exceptions.AMQPConnectionError as exc:
            raise MessageMiddlewareDisconnectedError() from exc
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareMessageError() from exc

    def close(self):
        try:
            if self.channel is not None and self.channel.is_open:
                self.channel.close()
            if self.connection is not None and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPError as exc:
            raise MessageMiddlewareCloseError() from exc
