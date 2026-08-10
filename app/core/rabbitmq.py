import json
import pika
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class RabbitMQPool:
    """Pool de conexão única para RabbitMQ (reutilizada)."""
    
    _instance: Optional['RabbitMQPool'] = None
    _connection: Optional[pika.BlockingConnection] = None
    
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
    
    def get_connection(self) -> pika.BlockingConnection:
        """Retorna conexão existente ou cria nova."""
        # Se conexão está aberta, reutiliza
        if self._is_connection_open():
            return self._connection
        
        # Caso contrário, cria nova
        logger.info("Criando nova conexão com RabbitMQ...")
        try:
            credentials = pika.PlainCredentials("guest", "guest")
            parameters = pika.ConnectionParameters(
                host="rabbitmq",
                credentials=credentials,
                socket_timeout=5,
                connection_attempts=3,
                retry_delay=2,
            )
            self._connection = pika.BlockingConnection(parameters)
            logger.info("✓ Conexão estabelecida com RabbitMQ")
            return self._connection
        except Exception as e:
            logger.error(f"✗ Falha ao conectar: {e}")
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
    pool = RabbitMQPool()
    
    for attempt in range(1, max_retries + 1):
        try:
            connection = pool.get_connection()
            channel = connection.channel()
            
            channel.basic_publish(
                exchange="",
                routing_key="webhook_queue",
                body=json.dumps(message),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                )
            )
            logger.info(f"✓ Mensagem publicada: {message}")
            return
            
        except (pika.exceptions.AMQPConnectionError, 
                pika.exceptions.AMQPChannelError,
                ConnectionResetError) as e:
            logger.warning(f"Tentativa {attempt}/{max_retries} falhou: {e}")
            pool.reset_connection()
            
            if attempt == max_retries:
                logger.error("Falha ao publicar após todas as tentativas")
                raise
        except Exception as e:
            logger.error(f"Erro inesperado: {e}")
            raise