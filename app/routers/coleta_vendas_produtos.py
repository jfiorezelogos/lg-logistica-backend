from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# ajuste o import conforme onde você salvou o helper
from services.mapeamento import load_skus_info

from app.services.coleta_vendas_produtos import iniciar_coleta_vendas_produtos

router = APIRouter(prefix="/produtos", tags=["Produtos"])


class ColetaProdutosIn(BaseModel):
    data_ini: date
    data_fim: date
    nome_produto: str | None = None


class ColetaProdutosOut(BaseModel):
    modo: str
    inicio: str
    fim: str
    produtos_ids: list[str]


@router.post("/coletar", response_model=ColetaProdutosOut)
def coletar_produtos(req: ColetaProdutosIn) -> ColetaProdutosOut:
    try:
        skus_info = load_skus_info()  # carrega do skus.json (ou fonte real do domínio)

        payload = iniciar_coleta_vendas_produtos(
            data_ini=req.data_ini,
            data_fim=req.data_fim,
            nome_produto=req.nome_produto,
            skus_info=skus_info,
        )

        # filtra para o shape do response_model
        return ColetaProdutosOut(
            modo=payload["modo"],
            inicio=payload["inicio"],
            fim=payload["fim"],
            produtos_ids=payload["produtos_ids"],
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha ao iniciar coleta de produtos: {e}")
