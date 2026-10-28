import json
import logging
import time
import pika
import signal
import sys

from urllib.parse import parse_qs

from app.core import config
from app.core.odoo import forward_to_odoo, TemporaryError, PermanentError
from app.core.rabbitmq_setup import setup_queue_with_dlq, WEBHOOK_QUEUE, WEBHOOK_RETRY_QUEUE

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def connect_with_retry(max_retries=10, initial_delay=2):
    """Conecta com retry exponencial."""
    delay = initial_delay

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries}...")
            conexao = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=config.RABBITMQ_HOST,
                    port=config.RABBITMQ_PORT,
                    credentials=pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD),
                    socket_timeout=5,
                )
            )
            logger.info("✓ Conectado!")
            return conexao

        except Exception as e:
            logger.warning(f"✗ Falha: {e}")

            if attempt == max_retries:
                logger.error("Impossível conectar. Encerrando.")
                raise  # Deixa container morrer (Docker reinicia)

            logger.info(f"Aguardando {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 60)  # Máximo 60s, dobrando a cada tentativa


def process_bitrix(ch, method, properties, body, mensagem):
    """Repassa o webhook do Bitrix para o Odoo, com retry se o Odoo estiver fora."""
    tentativas = (properties.headers or {}).get("x-retry-count", 0)
    evento = parse_qs(mensagem["body"]).get("event", ["?"])[0]
    logger.info(f"Bitrix → Odoo: evento {evento} (tentativa {tentativas + 1})")

    try:
        status = forward_to_odoo(mensagem)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info(f"✓ Entregue ao Odoo (HTTP {status})")

    except TemporaryError as e:
        if tentativas < config.ODOO_MAX_RETRIES:
            # Manda uma cópia para a fila de retry; depois do TTL ela volta para a fila principal
            ch.basic_publish(
                exchange="",
                routing_key=WEBHOOK_RETRY_QUEUE,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers={"x-retry-count": tentativas + 1},
                ),
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.warning(f"⏳ Odoo indisponível ({e}). Nova tentativa agendada.")
        else:
            logger.error(f"✗ Odoo indisponível após {tentativas + 1} tentativas ({e}). Enviando para a DLQ.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except PermanentError as e:
        logger.error(f"✗ Odoo recusou a mensagem ({e}). Enviando para a DLQ.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def callback(ch, method, properties, body):
    """Processa mensagem COM tratamento de erro."""
    try:
        mensagem = json.loads(body)

        if mensagem.get("source") == "bitrix":
            process_bitrix(ch, method, properties, body, mensagem)
            return

        logger.info(f"Processando: {mensagem}")
        # TODO: Adicionar lógica de processamento

        # Acknowledge (só após sucesso)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("✓ Processada com sucesso")

    except json.JSONDecodeError as e:
        # Mensagem inválida: rejeita sem requeue → vai para a DLQ
        logger.error(f"JSON inválido: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    except Exception as e:
        # Sem requeue → vai para a DLQ. Com requeue=True, uma mensagem que
        # sempre falha voltaria para a fila e travaria o worker num loop infinito.
        logger.error(f"Erro ao processar: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def main():
    conexao = connect_with_retry()
    canal = conexao.channel()

    def signal_handler(sig, frame):
        """Captura SIGTERM/SIGINT para parar suavemente."""
        logger.info(f"📍 Sinal {sig} recebido. Parando gracefully...")
        # Pede ao loop do pika para parar depois da mensagem atual
        conexao.add_callback_threadsafe(canal.stop_consuming)

    # Registrar handlers de sinal
    signal.signal(signal.SIGTERM, signal_handler)  # Docker envia SIGTERM
    signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C local

    # Usar a função que cria fila + DLX + DLQ
    setup_queue_with_dlq(canal)

    # Entrega uma mensagem por vez para este worker
    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(
        queue=WEBHOOK_QUEUE,
        on_message_callback=callback,
        auto_ack=False
    )

    logger.info("Aguardando mensagens...")

    try:
        canal.start_consuming()
    finally:
        logger.info("Fechando conexão...")
        if canal.is_open:
            canal.close()
        if conexao.is_open:
            conexao.close()
        logger.info("✓ Conexão fechada. Worker encerrado.")


if __name__ == "__main__":
    main()
