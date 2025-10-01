# app/services/guru_produtos.py
from __future__ import annotations

from typing import Any, cast

from app.common.http_client import http_get
from app.common.settings import settings
from app.services.guru_client import BASE_URL_GURU


def coletar_produtos_guru(limit: int = 100, cursor: str | None = None) -> dict[str, Any]:
    """
    Busca produtos no Guru (paginado).
    Retorna o payload bruto da API do Guru.
    """
    url = f"{BASE_URL_GURU.rstrip('/')}/products"
    headers = {"Authorization": f"Bearer {settings.API_KEY_GURU}"}
    params: dict[str, Any] = {"limit": limit}
    if cursor:
        params["cursor"] = cursor

    r = http_get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    return cast(dict[str, Any], r.json())
