from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query

from app.schemas.guru_vendas_assinaturas import ColetaOut
from app.services.guru_vendas_assinaturas import montar_payload_busca_assinaturas
from app.services.guru_worker_coleta import executar_worker_guru
from app.services.loader_main import carregar_cfg, carregar_skus
from app.services.loader_regras_assinaturas import (
    montar_mapas_cupons,
    montar_ofertas_embutidas,
    normalizar_rules,
)

router = APIRouter(prefix="/guru/pedidos", tags=["Coleta"])

# --- caminhos ---
BASE_DIR = Path(__file__).resolve().parents[2]
SKUS_PATH = BASE_DIR / "skus.json"
CFG_PATH = BASE_DIR / "config_ofertas.json"

# --- cache exclusivo desta rota (guarda apenas o último snapshot) ---
_CACHE_ASSINATURAS: dict[str, tuple[list[dict[str, Any]], dict[str, dict[str, int]]]] = {}


@router.get(
    "/assinaturas",
    response_model=ColetaOut,
    summary="Coletar assinaturas (retorno completo; paginação feita no front)",
    description=(
        "Executa a **coleta completa** e retorna **todas as linhas** em `linhas`, junto com `contagem`.\n\n"
        "- **Cache desta rota**: guarda apenas o **último snapshot** coletado; cada nova chamada sobrescreve.\n"
        "- **Paginação**: deve ser aplicada **exclusivamente no frontend** (ex.: fatiando `linhas`)."
    ),
    response_description="Retorna todas as linhas coletadas (`linhas`) e o resumo (`contagem`).",
)
def coletar_assinaturas(
    ano: int,  # ex.: 2025
    mes: int,  # 1..12
    box_sku: str,  # ex.: "C002A"
    modo_periodo: int = Query(
        ..., ge=0, le=1, description="Modo do período: **1 = PERÍODO**, **0 = TODAS**", example=1
    ),
    periodicidade: Literal["mensal", "bimestral"] = Query(
        ..., description='Periodicidade: "mensal" ou "bimestral"', example="mensal"
    ),
) -> ColetaOut:
    try:
        skus_info = carregar_skus()
        cfg = carregar_cfg()

        # resolve nome do box pelo SKU
        box_entry = next((nome for nome, info in skus_info.items() if info.get("sku") == box_sku), None)
        if not box_entry:
            raise HTTPException(status_code=400, detail=f"SKU '{box_sku}' não encontrado no skus.json")

        # traduz modo (0/1 -> "TODAS"/"PERÍODO")
        modo_str = "PERÍODO" if modo_periodo == 1 else "TODAS"

        # derivados de config
        ofertas_embutidas = montar_ofertas_embutidas(cfg)
        cupons_cdf, cupons_bi_mens = montar_mapas_cupons(cfg)

        # monta payload
        dados = montar_payload_busca_assinaturas(
            ano=ano,
            mes=mes,
            modo_periodo=modo_str,
            box_nome=box_entry,
            periodicidade=periodicidade,
            skus_info=skus_info,
        )
        dados["rules"] = normalizar_rules(cfg)
        dados["ofertas_embutidas"] = ofertas_embutidas
        dados["cupons_personalizados_cdf"] = cupons_cdf
        dados["cupons_personalizados_bi_mens"] = cupons_bi_mens
        dados["cupons_personalizados_anual"] = cupons_cdf
        dados["cupons_personalizados_bimestral"] = cupons_bi_mens

        # coleta nova e substitui cache anterior (mantém só o último snapshot)
        linhas, contagem = executar_worker_guru(dados, skus_info=skus_info)
        _CACHE_ASSINATURAS["last"] = (linhas, contagem)

        # retorno completo (sem paginação no backend)
        return ColetaOut(linhas=linhas, contagem=contagem)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta: {e!s}")
