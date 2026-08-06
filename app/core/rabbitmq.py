import json
import pika

RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "webhook_queue"


def publish_message(message):

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )

    channel = connection.channel()

    channel.queue_declare(
        queue=QUEUE_NAME,
        durable=True
    )

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(message),
    )

    connection.close()