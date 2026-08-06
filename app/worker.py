import json
import pika


def callback(ch, method, properties, body):
    mensagem = json.loads(body)

    print("=" * 50)
    print("Mensagem recebida:")
    print(mensagem)
    print("=" * 50)

    ch.basic_ack(delivery_tag=method.delivery_tag)


conexao = pika.BlockingConnection(
    pika.ConnectionParameters("rabbitmq")
)

canal = conexao.channel()

canal.queue_declare(queue="webhook_queue")

canal.basic_consume(
    queue="webhook_queue",
    on_message_callback=callback,
)

print("Aguardando mensagens...")

canal.start_consuming()