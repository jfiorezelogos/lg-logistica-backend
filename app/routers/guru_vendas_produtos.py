from __future__ import annotations

from datetime import date
from typing import Optional, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.loader_produtos_info import load_skus_info
from app.services.guru_vendas_produtos import iniciar_coleta_vendas_produtos
from app.services.guru_worker_coleta import executar_worker_guru

router = APIRouter(prefix="/guru/vendas", tags=["Produtos"])


class ColetaOut(BaseModel):
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]


@router.get("/produtos", response_model=ColetaOut)
def coletar_vendas_produtos(
    data_ini: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_fim: date = Query(..., description="Data final (YYYY-MM-DD)"),
    nome_produto: Optional[str] = Query(None, description="Nome do produto ou vazio para todos"),
) -> ColetaOut:
    try:
        # 1) carregar SKUs
        skus_info = load_skus_info()

        # 2) montar payload (inicio/fim/produtos_ids)
        payload = iniciar_coleta_vendas_produtos(
            data_ini=data_ini,
            data_fim=data_fim,
            nome_produto=nome_produto,
            skus_info=skus_info,
        )

        # 3) executar coleta de fato (worker unificado)
        linhas, contagem = executar_worker_guru(payload, skus_info=skus_info)

        return ColetaOut(linhas=linhas, contagem=contagem)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta de vendas de produtos: {e}")
