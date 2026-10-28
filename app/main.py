from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from app.core.rabbitmq import publish_message, RabbitMQPool
from app.core.rabbitmq_setup import setup_queue_with_dlq
from app.core.health import check_rabbitmq_health
from pydantic import BaseModel, ConfigDict, Field
import logging
import sys
import time

# Logging direto para stdout (para docker logs capturar)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)


def setup_rabbitmq():
    """Configura filas do RabbitMQ na inicialização com retry automático."""
    logger.info("=" * 60)
    logger.info("🔧 INICIANDO CONFIGURAÇÃO DE FILAS DO RABBITMQ")
    logger.info("=" * 60)

    max_retries = 10
    retry_delay = 2
    pool = RabbitMQPool()

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries}...")
            with pool.lock:
                logger.info("1️⃣  Conectando ao RabbitMQ...")
                connection = pool.get_connection()
                logger.info("   ✓ Conexão estabelecida")

                logger.info("2️⃣  Criando canal...")
                channel = connection.channel()
                logger.info("   ✓ Canal criado")

                logger.info("3️⃣  Configurando fila com DLQ...")
                setup_queue_with_dlq(channel)
                channel.close()
                logger.info("   ✓ Fila configurada")

            logger.info("=" * 60)
            logger.info("✅ FILAS CONFIGURADAS COM SUCESSO!")
            logger.info("=" * 60)
            return  # Sucesso! Sai da função

        except Exception as e:
            logger.error(f"   ❌ Tentativa {attempt} falhou: {type(e).__name__}: {e}")

            if attempt == max_retries:
                logger.error("=" * 60)
                logger.error("❌ ERRO AO CONFIGURAR FILAS - ESGOTADAS AS TENTATIVAS!")
                logger.error("=" * 60)
                logger.error(f"Último erro: {e}", exc_info=True)
                raise

            logger.info(f"   ⏳ Aguardando {retry_delay}s antes de tentar novamente...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 1.5, 10)  # Backoff exponencial, máximo 10s


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Código antes do `yield` roda no startup; depois do `yield`, no shutdown."""
    setup_rabbitmq()
    yield
    with RabbitMQPool.lock:
        RabbitMQPool().close()
    logger.info("Conexão com RabbitMQ fechada")


app = FastAPI(lifespan=lifespan)


class WebhookPayload(BaseModel):
    """ Define o contrato de dados esperado. """

    event_type: str = Field(..., min_length=1)    # Obrigatório
    user_id: int = Field(..., gt=0)               # Deve ser > 0
    data: dict = Field(default_factory=dict)      # Opcional, default {}

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "event_type": "user_created",
                "user_id": 123,
                "data": {
                    "email": "test@example.com"
                }
            }
        }
    )

@app.get("/")
def home():
    return {"status": "online"}


@app.post("/webhook")
def webhook(payload: WebhookPayload):  # FastAPI valida automaticamente
    """
    Processa webhook.

    Se payload inválido → erro 422 automático (sem seu código fazer nada!)
    Se válido → você recebe objeto tipado (autocomplete no IDE)
    """
    # Agora você SABE que:
    # - payload.event_type existe e é string não-vazia
    # - payload.user_id existe e é inteiro > 0

    publish_message(payload.model_dump())
    return {"status": "success"}

@app.get("/health")
def health():
    """Health check da API (inclui a conexão com o RabbitMQ)."""
    rabbitmq_ok = check_rabbitmq_health()
    body = {
        "status": "healthy" if rabbitmq_ok else "unhealthy",
        "service": "fastapi-webhook-api",
        "rabbitmq": "ok" if rabbitmq_ok else "down",
    }
    # 503 faz o healthcheck do Docker marcar o container como unhealthy
    return JSONResponse(content=body, status_code=200 if rabbitmq_ok else 503)
