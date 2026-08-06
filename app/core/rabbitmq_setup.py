# app/core/rabbitmq_setup.py
"""Configuração de exchanges, queues e bindings."""

import pika
import logging

logger = logging.getLogger(__name__)

WEBHOOK_QUEUE = "webhook_queue"
WEBHOOK_DLX = "webhook_dlx"  # Dead Letter Exchange
WEBHOOK_DLQ = "webhook_dlq"  # Dead Letter Queue

def setup_queue_with_dlq(channel: pika.channel.Channel):
    """Cria fila com Dead Letter Queue."""
    
    # 1. Criar Dead Letter Exchange
    channel.exchange_declare(
        exchange=WEBHOOK_DLX,
        exchange_type='direct',
        durable=True
    )
    logger.info(f"Exchange '{WEBHOOK_DLX}' criado")
    
    # 2. Criar Dead Letter Queue
    channel.queue_declare(
        queue=WEBHOOK_DLQ,
        durable=True
    )
    logger.info(f"Fila DLQ '{WEBHOOK_DLQ}' criada")
    
    # 3. Conectar DLQ ao DLX
    channel.queue_bind(
        exchange=WEBHOOK_DLX,
        queue=WEBHOOK_DLQ,
        routing_key=WEBHOOK_QUEUE  # Quando mensagem for rejeitada, vai aqui
    )
    logger.info(f"DLQ bound ao DLX com routing_key '{WEBHOOK_QUEUE}'")
    
    # 4. Criar fila normal COM referência ao DLX
    channel.queue_declare(
        queue=WEBHOOK_QUEUE,
        durable=True,
        arguments={
            'x-dead-letter-exchange': WEBHOOK_DLX,  # ← Crucial!
            'x-dead-letter-routing-key': WEBHOOK_QUEUE
        }
    )
    logger.info(f"Fila '{WEBHOOK_QUEUE}' criada com DLX configurado")


def consume_from_dlq(channel: pika.channel.Channel, callback):
    """Consome mensagens da fila de erros para análise."""
    
    channel.basic_consume(
        queue=WEBHOOK_DLQ,
        on_message_callback=callback,
        auto_ack=False
    )
    logger.info("Listening to DLQ...")
    channel.start_consuming()