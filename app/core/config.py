# app/core/config.py
"""Configurações lidas de variáveis de ambiente (com defaults para dev local)."""

import os

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

# URL do Odoo que recebia o webhook do Bitrix diretamente (o "plano B")
ODOO_WEBHOOK_URL = os.getenv("ODOO_WEBHOOK_URL", "")
ODOO_TIMEOUT_SECONDS = float(os.getenv("ODOO_TIMEOUT_SECONDS", "30"))
# Quantas vezes tentar de novo quando o Odoo estiver fora antes de mandar para a DLQ
ODOO_MAX_RETRIES = int(os.getenv("ODOO_MAX_RETRIES", "5"))
