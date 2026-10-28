# app/core/odoo.py
"""Repasse do webhook do Bitrix para o Odoo, exatamente como ele chegou."""

import logging

import requests

from app.core import config

logger = logging.getLogger(__name__)


class TemporaryError(Exception):
    """Falha que pode se resolver sozinha (Odoo fora, timeout, erro 5xx): vale tentar de novo."""


class PermanentError(Exception):
    """Falha que não adianta repetir (erro 4xx: o Odoo recusou os dados)."""


def forward_to_odoo(message: dict) -> int:
    """Envia o corpo original para a URL do Odoo. Retorna o status HTTP em caso de sucesso."""
    if not config.ODOO_WEBHOOK_URL:
        # Temporário de propósito: a mensagem fica tentando até alguém configurar a URL
        raise TemporaryError("ODOO_WEBHOOK_URL não configurada")

    url = config.ODOO_WEBHOOK_URL
    if message.get("query_string"):
        separador = "&" if "?" in url else "?"
        url = f"{url}{separador}{message['query_string']}"

    headers = {}
    if message.get("content_type"):
        headers["Content-Type"] = message["content_type"]

    try:
        resposta = requests.post(
            url,
            data=message["body"].encode("utf-8"),  # Mesmos bytes que o Bitrix enviou
            headers=headers,
            timeout=config.ODOO_TIMEOUT_SECONDS,
        )
    except requests.RequestException as e:
        # Sem resposta: conexão recusada, DNS, timeout...
        raise TemporaryError(f"{type(e).__name__}: {e}") from e

    if resposta.status_code >= 500 or resposta.status_code == 429:
        raise TemporaryError(f"Odoo respondeu {resposta.status_code}")
    if resposta.status_code >= 400:
        raise PermanentError(f"Odoo respondeu {resposta.status_code}: {resposta.text[:200]}")

    return resposta.status_code
