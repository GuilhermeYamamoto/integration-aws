# integration-aws

Recebe o webhook de saída do Bitrix, enfileira no RabbitMQ e o worker repassa ao Odoo,
com retry quando o Odoo está fora.

```
Bitrix ──POST /api/bitrix/post_crm_bitrix──▶ FastAPI ──▶ webhook_queue ──▶ Worker ──▶ Odoo
                                                           ▲      │
                                     (Odoo fora: espera 30s)│      │ (erro definitivo ou
                                                    webhook_retry ◀┘  tentativas esgotadas)
                                                                   ▼
                                                   webhook_dlx ──▶ webhook_dlq
```

O worker **não altera nada**: envia ao Odoo o mesmo corpo, `Content-Type` e query string
que o Bitrix enviou. Por isso o Odoo funciona igual pelos dois caminhos.

## Plano B: voltar a enviar direto para o Odoo

O endpoint do FastAPI usa **o mesmo caminho** da URL do Odoo. Para trocar, basta mudar o
domínio no webhook de saída do Bitrix:

| Situação | URL no webhook de saída do Bitrix |
|---|---|
| Normal (com fila) | `https://<domínio-do-fastapi>/api/bitrix/post_crm_bitrix` |
| Plano B (direto, como antes) | `https://<domínio-do-odoo>/api/bitrix/post_crm_bitrix` |

Ao voltar do plano B para o FastAPI, confira no painel do RabbitMQ se ficaram mensagens
antigas em `webhook_queue` / `webhook_retry` antes de religar o worker: elas seriam
entregues ao Odoo atrasadas.

## Estrutura

| Arquivo | Função |
|---|---|
| `app/main.py` | API FastAPI: recebe o webhook do Bitrix e publica na fila; `/health` checa o RabbitMQ |
| `app/worker.py` | Consome `webhook_queue`, repassa ao Odoo e agenda retry ou manda para a DLQ |
| `app/core/odoo.py` | Envio HTTP para o Odoo e classificação de erros (temporário × definitivo) |
| `app/core/config.py` | Lê configurações das variáveis de ambiente |
| `app/core/rabbitmq.py` | Conexão compartilhada com o RabbitMQ e `publish_message` |
| `app/core/rabbitmq_setup.py` | Declara fila principal, fila de retry, Dead Letter Exchange e Dead Letter Queue |
| `app/core/health.py` | Verificação de saúde da conexão com o RabbitMQ |

## Rodando

```bash
cp .env.example .env
```

Preencha `ODOO_WEBHOOK_URL` no `.env` com a URL do Odoo que estava no Bitrix e suba:

```bash
docker compose up --build
```

- API e docs interativas: http://localhost:8000/docs
- Painel do RabbitMQ: http://localhost:15672 (usuário/senha do `.env`)

## No servidor (produção)

Preencha também `DOMAIN` e troque `RABBITMQ_PASSWORD` no `.env`, e suba com o perfil `prod`,
que inclui o Caddy (HTTPS automático na frente do FastAPI):

```bash
docker compose --profile prod up -d --build
```

- Públicos (via HTTPS): só `/api/bitrix/*` e `/health`. O resto responde 404.
- FastAPI (8000) e RabbitMQ (5672/15672) ficam presos ao `127.0.0.1` do servidor. Para ver o
  painel, use um túnel SSH: `ssh -L 15672:localhost:15672 usuario@servidor` e abra
  http://localhost:15672 no seu computador.

## Comportamento de erros no repasse ao Odoo

| Resposta do Odoo | O que acontece |
|---|---|
| 2xx | Mensagem confirmada (ack) e removida da fila |
| Sem resposta, timeout, 5xx ou 429 | Vai para `webhook_retry`, volta após 30s; após `ODOO_MAX_RETRIES` tentativas extras, vai para a DLQ |
| 4xx | Vai direto para a DLQ (repetir não resolveria) |
