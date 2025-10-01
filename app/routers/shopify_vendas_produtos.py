from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas.shopify_vendas_produtos import ColetaProdutosIn, ColetaProdutosOut
from app.services.shopify_vendas_produtos import coletar_vendas_shopify

router = APIRouter(prefix="/shopify/vendas", tags=["Coleta"])

# app/routers/shopify_vendas_produtos.py


@router.post("/produtos", response_model=ColetaProdutosOut, summary="Coletar vendas de produtos (tudo-em-um)")
def coletar_produtos_unico(body: ColetaProdutosIn) -> ColetaProdutosOut:
    try:
        # opcional: implementar um provider real aqui:
        ai_provider = None  # ex.: lambda prompt: openai_client.chat.completions.create(...)

        linhas, contagem = coletar_vendas_shopify(
            data_inicio=body.data_inicio,
            fulfillment_status=body.fulfillment_status,
            produto_alvo=body.produto_alvo,
            ids_shopify=body.ids_shopify,
            enrich_cpfs=body.enrich_cpfs,
            enrich_bairros=body.enrich_bairros,
            enrich_enderecos=body.enrich_enderecos,
            use_ai_enderecos=body.use_ai_enderecos,
            ai_provider=ai_provider,
        )
        return ColetaProdutosOut(linhas=linhas, contagem=contagem)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha na coleta: {e}")
