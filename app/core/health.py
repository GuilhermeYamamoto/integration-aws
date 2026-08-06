# app/core/health.py
import pika
import logging

logger = logging.getLogger(__name__)

def check_rabbitmq_health() -> bool:
    """Verifica se RabbitMQ está respondendo."""
    try:
        from app.core.rabbitmq import RabbitMQPool
        
        pool = RabbitMQPool()
        conexao = pool.get_connection()
        
        # Simples: se conseguiu conexão, está OK
        if conexao and conexao.is_open:
            logger.debug("✓ RabbitMQ health check: OK")
            return True
        else:
            logger.warning("✗ RabbitMQ health check: conexão não está aberta")
            return False
            
    except Exception as e:
        logger.error(f"✗ RabbitMQ health check falhou: {e}")
        return False