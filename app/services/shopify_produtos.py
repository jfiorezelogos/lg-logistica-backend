from __future__ import annotations

from typing import Any

from app.common.http_client import http_get
from app.common.settings import settings
from app.schemas.shopify_produtos import ProductShopifyVariant
from app.services.shopify_client import API_VERSION


def buscar_produtos_shopify() -> list[ProductShopifyVariant]:
    """
    Consulta a API REST da Shopify e retorna uma lista plana de variantes de produtos
    com product_id, variant_id, title e sku. Paginação automática (limit=250).
    """
    url: str | None = f"https://{settings.SHOP_URL}/admin/api/{API_VERSION}/products.json?limit=250"
    headers: dict[str, str] = {
        "X-Shopify-Access-Token": settings.SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }

    todos: list[ProductShopifyVariant] = []
    pagina_atual: int = 1

    while url:
        # ❌ remove verify=False — seu http_get já deve validar TLS por padrão
        resp = http_get(url, headers=headers)
        if resp.status_code != 200:
            print(f"❌ Erro Shopify {resp.status_code}: {resp.text}")
            break

        produtos_json: list[dict[str, Any]] = resp.json().get("products", []) or []
        print(f"📄 Página {pagina_atual}: {len(produtos_json)} produtos retornados")

        for produto in produtos_json:
            id_produto_any: Any = produto.get("id")
            if not id_produto_any:
                continue
            try:
                id_produto: int = int(id_produto_any)
            except (TypeError, ValueError):
                continue

            titulo_produto: str = str(produto.get("title", "")).strip()
            variants: list[dict[str, Any]] = produto.get("variants", []) or []

            for variante in variants:
                variant_id_any: Any = variante.get("id")
                if not variant_id_any:
                    continue
                try:
                    variant_id: int = int(variant_id_any)
                except (TypeError, ValueError):
                    continue

                sku: str = str(variante.get("sku", "")).strip()

                todos.append(
                    ProductShopifyVariant(
                        product_id=id_produto,
                        variant_id=variant_id,
                        title=titulo_produto,
                        sku=sku,
                    )
                )

        pagina_atual += 1

        # paginação via header "Link"
        link: str = resp.headers.get("Link", "") or ""
        if 'rel="next"' in link:
            partes = [p.strip() for p in link.split(",")]
            next_url_parts = [p.split(";")[0].strip().strip("<>") for p in partes if 'rel="next"' in p]
            url = next_url_parts[0] if next_url_parts else None
        else:
            break

    return todos
