from fastapi import FastAPI
from app.core.rabbitmq import publish_message, RabbitMQPool
from app.core.rabbitmq_setup import setup_queue_with_dlq
from pydantic import BaseModel, Field
import logging
import sys

# Logging direto para stdout (para docker logs capturar)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Inicializar filas ao startup com retry
@app.on_event("startup")
def startup_event():
    """Configura filas do RabbitMQ na inicialização com retry automático."""
    logger.info("=" * 60)
    logger.info("🔧 INICIANDO CONFIGURAÇÃO DE FILAS DO RABBITMQ")
    logger.info("=" * 60)
    
    import time
    max_retries = 10
    retry_delay = 2
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Tentativa {attempt}/{max_retries}...")
            logger.info("1️⃣  Obtendo pool de conexão...")
            pool = RabbitMQPool()
            
            logger.info("2️⃣  Conectando ao RabbitMQ...")
            connection = pool.get_connection()
            logger.info("   ✓ Conexão estabelecida")
            
            logger.info("3️⃣  Criando canal...")
            channel = connection.channel()
            logger.info("   ✓ Canal criado")
            
            logger.info("4️⃣  Configurando fila com DLQ...")
            setup_queue_with_dlq(channel)
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

class WebhookPayload(BaseModel):
    """ Define o contrado de dados esperado. """

    event_type: str = Field(..., min_length=1)    # Obrigatório
    user_id: int = Field(..., gt=0)               # Deve ser > 0
    data: dict = Field(default_factory=dict)      # Opcional, default {}

    class Config:
        json_schema_extra = {
            "example": {
                "event_type": "user_created",
                "user_id": 123,
                "data": {
                    "email": "test@example.com"
                }
            }
        }

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
    
    publish_message(payload.dict())
    return {"status": "success"}

@app.get("/health")
def health():
    """Health check da API."""
    return {
        "status": "healthy",
        "service": "fastapi-webhook-api"
    }