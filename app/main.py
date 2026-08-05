from fastapi import FastAPI
from app.core.rabbitmq import publish_message

app = FastAPI()


@app.get("/")
def home():
    return {"status": "online"}


@app.post("/webhook")
def webhook(payload: dict):

    publish_message(payload)

    return {
        "status": "success"
    }