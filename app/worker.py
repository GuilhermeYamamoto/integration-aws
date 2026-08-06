import json
import time
import pika


def callback(ch, method, properties, body):
    mensagem = json.loads(body)

    print("=" * 50)
    print("Mensagem recebida:")
    print(mensagem)
    print("=" * 50)

    ch.basic_ack(delivery_tag=method.delivery_tag)


while True:
    try:
        print("Tentando conectar ao RabbitMQ...")

        conexao = pika.BlockingConnection(
            pika.ConnectionParameters("rabbitmq")
        )

        print("Conectado ao RabbitMQ.")
        break

    except Exception as e:
        print(f"Erro: {e}")
        time.sleep(5)


canal = conexao.channel()

canal.queue_declare(
    queue="webhook_queue",
    durable=True
)

canal.basic_consume(
    queue="webhook_queue",
    on_message_callback=callback,
)

print("Aguardando mensagens...")

canal.start_consuming()