import json
import logging
import time
import pika
import signal
import sys

from app.core.rabbitmq_setup import setup_queue_with_dlq

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variável global para controlar o shutdown
should_continue = True

def signal_handler(sig, frame):
    """Captura SIGTERM/SIGINT para parar suavemente."""
    global should_continue
    
    logger.info(f"\n📍 Sinal {sig} recebido. Parando gracefully...")
    should_continue = False

# Registrar handlers de sinal
signal.signal(signal.SIGTERM, signal_handler)  # Docker envia SIGTERM
signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C local

def connect_with_retry(max_retries=10, initial_delay=2):
    """Conecta com retry exponencial."""
    delay = initial_delay
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries}...")
            conexao = pika.BlockingConnection(
                pika.ConnectionParameters("rabbitmq", socket_timeout=5)
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


def callback(ch, method, properties, body):
    """Processa mensagem COM tratamento de erro."""
    try:
        mensagem = json.loads(body)
        
        logger.info(f"Processando: {mensagem}")
        # TODO: Adicionar lógica de processamento
        
        # Acknowledge (só após sucesso)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("✓ Processada com sucesso")
        
    except json.JSONDecodeError as e:
        # Mensagem inválida: rejeita sem requeue (descarta)
        logger.error(f"JSON inválido: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        
    except Exception as e:
        # Erro temporário: requeue (tenta novamente)
        logger.error(f"Erro ao processar: {e}", exc_info=True)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


# Conectar
conexao = connect_with_retry()
canal = conexao.channel()

# Usar a função que cria fila + DLX + DLQ
setup_queue_with_dlq(canal)

canal.basic_qos(prefetch_count=1)

# Resto normal
canal.basic_consume(
    queue="webhook_queue",
    on_message_callback=callback,
    auto_ack=False
)

logger.info("Aguardando mensagens...")

try:
    canal.start_consuming()
except KeyboardInterrupt:
    logger.info("Interrupção do usuário")
finally:
    logger.info("Fechando conexão...")
    if canal and canal.is_open:
        canal.close()
    if conexao and conexao.is_open:
        conexao.close()
    logger.info("✓ Conexão fechada. Worker encerrado.")