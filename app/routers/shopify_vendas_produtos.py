from fastapi import APIRouter, Query, HTTPException
from app.schemas.shopify_vendas_produtos import ColetaProdutosOut
from app.services.shopify_vendas_produtos import coletar_vendas_shopify

router = APIRouter(prefix="/shopify", tags=["Coleta"])

def _parse_sku_list(valor: str | None) -> list[str] | None:
    if not valor:
        return None
    partes = [p.strip().upper() for p in valor.split(",")]
    return [p for p in partes if p]

@router.get(
    "/pedidos",
    response_model=ColetaProdutosOut,
    summary="Coletar vendas de produtos na Shopify",
)
def coletar_vendas_shopify_endpoint(
    data_inicio: str = Query(..., description="Data inicial no formato dd/MM/yyyy"),
    fulfillment_status: str = Query("any", description='"any" ou "unfulfilled"'),
    sku_produtos: str | None = Query(
        None,
        description='SKU único ou CSV de SKUs (ex.: "L006A" ou "L006A,L007B")',
    ),
) -> ColetaProdutosOut:
    # se veio None ou "", fica None
    sku_list = None
    if sku_produtos:
        sku_list = [p.strip().upper() for p in sku_produtos.split(",") if p.strip()]

    fs = (fulfillment_status or "any").strip().lower()
    if fs not in {"any", "unfulfilled"}:
        raise HTTPException(status_code=400, detail='fulfillment_status deve ser "any" ou "unfulfilled"')

    linhas, contagem = coletar_vendas_shopify(
        data_inicio=data_inicio,
        fulfillment_status=fs,
        sku_produtos=sku_list,   # ← None = busca todos, lista = filtra
        use_ai_enderecos=True,
        ai_provider=None,
    )
    return ColetaProdutosOut(linhas=linhas, contagem=contagem)

