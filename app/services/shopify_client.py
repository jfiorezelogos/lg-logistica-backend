from __future__ import annotations

from datetime import UTC, datetime

from app.common.settings import settings


def obter_api_shopify_version(now: datetime | None = None) -> str:
    """
    Retorna a versão trimestral da Shopify API (YYYY-01/04/07/10).
    Usa datetime aware (UTC por padrão). 'now' é opcional (útil para testes).
    """
    dt = now or datetime.now(UTC)
    y, m = dt.year, dt.month
    q_start = ((m - 1) // 3) * 3 + 1  # 1, 4, 7, 10
    return f"{y}-{q_start:02d}"


API_VERSION = obter_api_shopify_version()
GRAPHQL_URL = f"https://{settings.SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
REST_URL = f"https://{settings.SHOP_URL}/admin/api/{API_VERSION}"  # útil para endpoints REST


def _graphql_url() -> str:
    return f"https://{settings.SHOP_URL}/admin/api/{obter_api_shopify_version()}/graphql.json"


def _http_shopify_headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": settings.SHOPIFY_TOKEN,
    }
