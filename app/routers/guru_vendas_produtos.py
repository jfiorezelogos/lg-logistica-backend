from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas.guru_vendas_produtos import ColetaOut
from app.services.guru_vendas_produtos import iniciar_coleta_vendas_produtos
from app.services.guru_worker_coleta import executar_worker_guru
from app.services.loader_produtos_info import load_skus_info

router = APIRouter(prefix="/guru/pedidos", tags=["Coletas"])

# --- cache exclusivo desta rota (guarda só o último snapshot) ---
_CACHE_PRODUTOS: dict[str, tuple[list[dict[str, Any]], dict[str, dict[str, int]]]] = {}


@router.get(
    "/produtos",
    response_model=ColetaOut,
    summary="Coletar vendas de produtos (retorno completo; paginação no front)",
    description=(
        "Executa a coleta completa e retorna **todas as linhas** em `linhas`, com `contagem`.\n\n"
        "- **Cache desta rota**: mantém apenas o **último snapshot**; cada nova chamada sobrescreve.\n"
        "- **Paginação**: deve ser feita **exclusivamente no frontend** (fatie `linhas` no cliente)."
    ),
    response_description="Retorna todas as linhas (`linhas`) e o resumo (`contagem`).",
)
def buscar_vendas_produtos_guru(
    data_ini: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_fim: date = Query(..., description="Data final (YYYY-MM-DD)"),
    nome_produto: str | None = Query(None, description="Nome do produto ou vazio para todos"),
) -> ColetaOut:
    try:
        # 1) carregar SKUs
        skus_info = load_skus_info()

        # 2) montar payload
        payload = iniciar_coleta_vendas_produtos(
            data_ini=data_ini,
            data_fim=data_fim,
            nome_produto=nome_produto,
            skus_info=skus_info,
        )

        # 3) coleta e sobrescreve o snapshot em cache
        linhas, contagem = executar_worker_guru(payload, skus_info=skus_info)
        _CACHE_PRODUTOS["last"] = (linhas, contagem)

        # 4) retorno completo (sem paginação no backend)
        return ColetaOut(linhas=linhas, contagem=contagem)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta de vendas de produtos: {e}")
