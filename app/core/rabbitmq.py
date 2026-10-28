import json
import pika
import logging
import threading
from typing import Optional

from app.core import config
from app.core.rabbitmq_setup import WEBHOOK_QUEUE

logger = logging.getLogger(__name__)

class RabbitMQPool:
    """Pool de conexão única para RabbitMQ (reutilizada).

    BlockingConnection NÃO é thread-safe e o FastAPI roda endpoints `def`
    em várias threads. Por isso, todo uso da conexão deve acontecer
    dentro de `with pool.lock:`.
    """

    _instance: Optional['RabbitMQPool'] = None
    _connection: Optional[pika.BlockingConnection] = None
    lock = threading.Lock()

    def __new__(cls):
        """Singleton: garante uma única instância."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def _is_connection_open(self) -> bool:
        """Verifica se conexão está realmente aberta."""
        try:
            return (self._connection and 
                    self._connection.is_open and 
                    self._connection.is_closed is False)
        except Exception as e:
            logger.warning(f"Erro ao verificar conexão: {e}")
            return False
    
    def get_connection(self, connection_attempts: int = 5) -> pika.BlockingConnection:
        """Retorna conexão existente ou cria nova (com até `connection_attempts` tentativas)."""
        # Se conexão está aberta, reutiliza
        if self._is_connection_open():
            logger.debug("♻️  Reutilizando conexão existente")
            return self._connection
        
        # Caso contrário, cria nova
        logger.info("🔗 Criando nova conexão com RabbitMQ...")
        try:
            credentials = pika.PlainCredentials(config.RABBITMQ_USER, config.RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=config.RABBITMQ_HOST,
                port=config.RABBITMQ_PORT,
                credentials=credentials,
                socket_timeout=5,
                connection_attempts=connection_attempts,
                retry_delay=3,
            )
            logger.info(f"   Conectando para {config.RABBITMQ_HOST}:{config.RABBITMQ_PORT}...")
            self._connection = pika.BlockingConnection(parameters)
            logger.info("   ✓ Conexão TCP estabelecida")
            logger.info("✅ Conexão RabbitMQ ativa e pronta")
            return self._connection
        except Exception as e:
            logger.error(f"❌ Falha ao conectar ao RabbitMQ: {type(e).__name__}: {e}")
            raise
    
    def reset_connection(self):
        """Reseta conexão (para reconectar em caso de erro)."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
        self._connection = None
    
    def close(self):
        """Fecha conexão (para graceful shutdown)."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None


def publish_message(message, max_retries=3):
    """Publica mensagem com retry automático."""
    # Não loga o conteúdo: o corpo do Bitrix traz o token de autenticação
    logger.info(f"📤 Publicando mensagem (origem: {message.get('source', 'webhook')})")
    pool = RabbitMQPool()
    
    for attempt in range(1, max_retries + 1):
        # Uma thread por vez usa a conexão compartilhada
        with pool.lock:
            try:
                logger.info(f"Tentativa {attempt}/{max_retries}...")
                connection = pool.get_connection()
                channel = connection.channel()
                try:
                    logger.info(f"Enviando para fila '{WEBHOOK_QUEUE}'...")
                    channel.basic_publish(
                        exchange="",
                        routing_key=WEBHOOK_QUEUE,
                        body=json.dumps(message),
                        properties=pika.BasicProperties(
                            delivery_mode=2,  # Mensagem persistente
                        )
                    )
                finally:
                    # Sempre fecha o canal, senão cada publicação deixa um aberto
                    if channel.is_open:
                        channel.close()
                logger.info("✅ Mensagem publicada com sucesso!")
                return

            except pika.exceptions.ChannelClosedByBroker as e:
                logger.error(f"❌ Canal fechado pela broker: {e}")
                pool.reset_connection()
                if attempt == max_retries:
                    raise
            except (pika.exceptions.AMQPConnectionError,
                    pika.exceptions.AMQPChannelError,
                    pika.exceptions.StreamLostError,
                    ConnectionResetError) as e:
                logger.warning(f"⚠️  Tentativa {attempt}/{max_retries} falhou: {type(e).__name__}: {e}")
                pool.reset_connection()
                if attempt == max_retries:
                    logger.error(f"❌ Falha ao publicar após {max_retries} tentativas")
                    raise
            except Exception as e:
                logger.error(f"❌ Erro inesperado: {type(e).__name__}: {e}", exc_info=True)
                raise