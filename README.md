# integration-aws

API que recebe webhooks e os processa de forma assíncrona via RabbitMQ.

```
Cliente ──POST /webhook──▶ FastAPI ──publica──▶ webhook_queue ──▶ Worker
                                                    │ (erro / rejeitada)
                                                    ▼
                                    webhook_dlx ──▶ webhook_dlq
```

## Estrutura

| Arquivo | Função |
|---|---|
| `app/main.py` | API FastAPI: valida o webhook e publica na fila; `/health` checa o RabbitMQ |
| `app/worker.py` | Consome `webhook_queue` e processa cada mensagem |
| `app/core/config.py` | Lê configurações das variáveis de ambiente |
| `app/core/rabbitmq.py` | Conexão compartilhada com o RabbitMQ e `publish_message` |
| `app/core/rabbitmq_setup.py` | Declara fila principal, Dead Letter Exchange e Dead Letter Queue |
| `app/core/health.py` | Verificação de saúde da conexão com o RabbitMQ |

## Rodando

```bash
cp .env.example .env
docker compose up --build
```

- API e docs interativas: http://localhost:8000/docs
- Painel do RabbitMQ: http://localhost:15672 (usuário/senha do `.env`)

Teste rápido:

```bash
curl -X POST http://localhost:8000/webhook -H "Content-Type: application/json" -d '{"event_type": "user_created", "user_id": 123}'
```
