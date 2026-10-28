# app/core/health.py
import logging

from app.core.rabbitmq import RabbitMQPool

logger = logging.getLogger(__name__)

def check_rabbitmq_health() -> bool:
    """Verifica se RabbitMQ está respondendo."""
    pool = RabbitMQPool()
    try:
        with pool.lock:
            # Uma tentativa só: o health precisa responder rápido, não esperar o broker voltar
            conexao = pool.get_connection(connection_attempts=1)
            # Troca dados com o broker sem esperar: se a conexão caiu, levanta exceção.
            # Também responde aos heartbeats, mantendo a conexão viva entre publicações.
            conexao.process_data_events(time_limit=0)
            logger.debug("✓ RabbitMQ health check: OK")
            return True

    except Exception as e:
        logger.error(f"✗ RabbitMQ health check falhou: {e}")
        with pool.lock:
            pool.reset_connection()
        return False
