from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.guru_vendas_assinaturas import montar_payload_busca_assinaturas
from app.services.guru_worker_coleta import executar_worker_guru
from app.services.loader_main import carregar_cfg, carregar_skus
from app.services.loader_regras_assinaturas import (
    montar_mapas_cupons,
    montar_ofertas_embutidas,
    normalizar_rules,
)

router = APIRouter(prefix="/guru/vendas", tags=["Coleta"])

# --- caminhos (ajuste se necessário) ---
BASE_DIR = Path(__file__).resolve().parents[2]
SKUS_PATH = BASE_DIR / "skus.json"
CFG_PATH = BASE_DIR / "config_ofertas.json"


class ColetaPage(BaseModel):
    items: list[dict[str, Any]] = Field(..., description="Linhas desta página")
    next_cursor: str | None = Field(None, description="Cursor para próxima página (se houver)")
    total: int = Field(..., description="Total de linhas da consulta (não paginado)")
    contagem: dict[str, dict[str, int]] = Field(..., description="Resumo/contagens")


class ColetaOut(BaseModel):  # mantém o schema de saída
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]


@router.get("/assinaturas", response_model=ColetaOut)
def coletar_assinaturas(
    ano: int = Query(..., ge=1900, le=2100, description="Ano do período (YYYY)"),
    mes: int = Query(..., ge=1, le=12, description="Mês do período (1-12)"),
    modo_periodo: str = Query(..., description='Use "PERÍODO" ou "TODAS" (aceita "PERIODO")'),
    periodicidade: str = Query(..., description='Periodicidade: "mensal" ou "bimestral"'),
    box_nome: str | None = Query(None, description="Nome do box (opcional)"),
) -> ColetaOut:
    try:
        # 1) carrega arquivos
        skus_info = carregar_skus()
        cfg = carregar_cfg()

        # 2) derivados de config
        ofertas_embutidas = montar_ofertas_embutidas(cfg)
        cupons_cdf, cupons_bi_mens = montar_mapas_cupons(cfg)

        # 3) payload base
        dados = montar_payload_busca_assinaturas(
            ano=ano,
            mes=mes,
            modo_periodo=modo_periodo,
            box_nome=box_nome,
            periodicidade=periodicidade,
            skus_info=skus_info,
        )

        # 4) injeta regras/mapas no payload
        dados["rules"] = normalizar_rules(cfg)
        dados["ofertas_embutidas"] = ofertas_embutidas
        dados["cupons_personalizados_cdf"] = cupons_cdf
        dados["cupons_personalizados_bi_mens"] = cupons_bi_mens
        # compat legada
        dados["cupons_personalizados_anual"] = cupons_cdf
        dados["cupons_personalizados_bimestral"] = cupons_bi_mens

        # 5) coleta e processa via worker unificado
        linhas, contagem = executar_worker_guru(dados, skus_info=skus_info)
        return ColetaOut(linhas=linhas, contagem=contagem)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta: {e!s}")
