from __future__ import annotations

from fastapi import APIRouter, Query

from app.schemas.shopify_vendas_produtos import ColetaProdutosOut
from app.services.shopify_vendas_produtos import coletar_vendas_shopify

router = APIRouter(prefix="/shopify", tags=["Coleta"])


def _parse_sku_list(valor: str | None) -> list[str] | None:
    """
    Aceita "L006A", "L006A,L007B" ou None. Normaliza para lista uppercase sem espaços.
    """
    if not valor:
        return None
    partes = [p.strip().upper() for p in valor.split(",")]
    return [p for p in partes if p]


@router.get(
    "/pedidos",
    response_model=ColetaProdutosOut,
    summary="Coletar vendas de produtos na Shopify",
)
def coletar_vendas_endpoint(
    data_inicio: str = Query(..., description="Data inicial no formato dd/MM/yyyy"),
    fulfillment_status: str = Query("any", description='"any" ou "unfulfilled"'),
    sku_produtos: str | None = Query(
        None,
        description='SKU ou lista separada por vírgula (ex.: "L006A" ou "L006A,L007B")',
    ),
) -> ColetaProdutosOut:
    skus = _parse_sku_list(sku_produtos)

    # Enriquecimentos sempre ligados
    linhas, contagem = coletar_vendas_shopify(
        data_inicio=data_inicio,
        fulfillment_status=fulfillment_status,
        # passa None para "todos" ou lista de SKUs quando houver filtro
        sku_produtos=skus,
        enrich_cpfs=True,
        enrich_bairros=True,
        enrich_enderecos=True,
        use_ai_enderecos=True,  # manter ligado; troque para False se quiser poupar IA
        ai_provider=None,  # injete seu provider se quiser IA server-side
    )
    return ColetaProdutosOut(linhas=linhas, contagem=contagem)
