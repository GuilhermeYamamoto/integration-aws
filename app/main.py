from fastapi import FastAPI
from app.core.rabbitmq import publish_message
from pydantic import BaseModel, Field

app = FastAPI()

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