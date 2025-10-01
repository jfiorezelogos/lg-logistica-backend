# app/routers/shopify_vendas_produtos.py
from fastapi import APIRouter

from app.schemas.shopify_vendas_produtos import ColetaProdutosIn, ColetaProdutosOut
from app.services.shopify_vendas_produtos import coletar_vendas_shopify

router = APIRouter(prefix="/shopify/vendas", tags=["shopify-vendas"])


@router.post("/produtos", response_model=ColetaProdutosOut, summary="Coletar vendas de produtos na Shopify")
def coletar_vendas_endpoint(payload: ColetaProdutosIn) -> ColetaProdutosOut:
    linhas, contagem = coletar_vendas_shopify(
        data_inicio=payload.data_inicio,
        fulfillment_status=payload.fulfillment_status,
        produto_alvo=payload.produto_alvo,
        ids_shopify=payload.ids_shopify,
        enrich_cpfs=payload.enrich_cpfs,
        enrich_bairros=payload.enrich_bairros,
        enrich_enderecos=payload.enrich_enderecos,
        use_ai_enderecos=payload.use_ai_enderecos,
        ai_provider=None,  # (opcional: injetar provider se quiser habilitar IA server-side)
    )
    return ColetaProdutosOut(linhas=linhas, contagem=contagem)
