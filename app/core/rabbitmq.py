import json
import pika
from typing import Optional

class RabbitMQPool:
    """Pool de conexão única para RabbitMQ (reutilizada)."""
    
    _instance: Optional['RabbitMQPool'] = None
    _connection: Optional[pika.BlockingConnection] = None
    
    def __new__(cls):
        """Singleton: garante uma única instância."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_connection(self) -> pika.BlockingConnection:
        """Retorna conexão existente ou cria nova."""
        # Se conexão está aberta, reutiliza
        if (self._connection and 
            self._connection.is_open and 
            self._connection.is_closed is False):
            return self._connection
        
        # Caso contrário, cria nova
        print("Criando nova conexão com RabbitMQ...")
        credentials = pika.PlainCredentials("guest", "guest")
        parameters = pika.ConnectionParameters(
            host="rabbitmq",
            credentials=credentials,
            socket_timeout=5,  # Evita hang indefinido
        )
        self._connection = pika.BlockingConnection(parameters)
        return self._connection
    
    def close(self):
        """Fecha conexão (para graceful shutdown)."""
        if self._connection and self._connection.is_open:
            self._connection.close()
            self._connection = None


# Usar assim:
def publish_message(message):
    pool = RabbitMQPool()
    connection = pool.get_connection()  # Reutiliza!
    channel = connection.channel()
    
    # Queue já foi declarada no startup, apenas publica
    channel.basic_publish(
        exchange="",
        routing_key="webhook_queue",
        body=json.dumps(message),
    )
    # NÃO fecha conexão! (para reutilizar)