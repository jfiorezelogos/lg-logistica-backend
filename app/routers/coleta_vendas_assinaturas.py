from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from services.coleta_guru import executar_worker_guru
from services.coleta_vendas_assinaturas import montar_payload_busca_assinaturas
from services.loader_main import carregar_cfg, carregar_skus
from services.loader_regras_assinaturas import montar_mapas_cupons, montar_ofertas_embutidas, normalizar_rules

router = APIRouter(prefix="/assinaturas", tags=["Assinaturas"])

# --- caminhos (ajuste se necessário) ---
BASE_DIR = Path(__file__).resolve().parents[2]  # raiz do projeto
SKUS_PATH = BASE_DIR / "skus.json"  # skus.json na raiz
CFG_PATH = BASE_DIR / "config_ofertas.json"  # config_ofertas.json na raiz


# ---- Schemas do endpoint (sem skus/rules no input) ----
class BuscarAssinaturasIn(BaseModel):
    ano: int = Field(..., ge=1900, le=2100)
    mes: int = Field(..., ge=1, le=12)
    modo_periodo: str  # "PERÍODO" | "TODAS" (aceita "PERIODO")
    box_nome: str | None = None
    periodicidade: str  # "mensal" | "bimestral"


class ColetaOut(BaseModel):
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]


@router.post("/coletar", response_model=ColetaOut)
def coletar_assinaturas(in_: BuscarAssinaturasIn) -> ColetaOut:
    try:
        # 1) carrega arquivos
        skus_info = carregar_skus()
        cfg = carregar_cfg()

        # 2) derivados de config
        ofertas_embutidas = montar_ofertas_embutidas(cfg)
        cupons_cdf, cupons_bi_mens = montar_mapas_cupons(cfg)

        # 3) payload base
        dados = montar_payload_busca_assinaturas(
            ano=in_.ano,
            mes=in_.mes,
            modo_periodo=in_.modo_periodo,
            box_nome=in_.box_nome,
            periodicidade=in_.periodicidade,
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
