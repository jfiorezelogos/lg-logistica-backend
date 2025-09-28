from typing import Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from pathlib import Path
from functools import lru_cache
import json
import io

from app.services.assinaturas import (
    montar_payload_busca_assinaturas,
    gerenciar_coleta_vendas_assinaturas,
    montar_planilha_vendas_guru
)

router = APIRouter(prefix="/assinaturas", tags=["Assinaturas"])

# --- caminhos (ajuste se necessário) ---
BASE_DIR = Path(__file__).resolve().parents[2]  # raiz do projeto
SKUS_PATH = BASE_DIR / "skus.json"              # skus.json na raiz
CFG_PATH  = BASE_DIR / "config_ofertas.json"    # config_ofertas.json na raiz

@lru_cache(maxsize=1)
def carregar_skus() -> dict[str, dict[str, Any]]:
    with io.open(SKUS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

@lru_cache(maxsize=1)
def carregar_cfg() -> dict[str, Any]:
    with io.open(CFG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def _normalizar_rules(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Garante que devolvemos uma lista de regras (suporta 'rules' e legado 'regras')."""
    r = cfg.get("rules")
    if r is None:
        r = cfg.get("regras")
    return r if isinstance(r, list) else []

def montar_ofertas_embutidas(cfg: dict[str, Any]) -> dict[str, str]:
    """
    Constrói {oferta_id: nome_do_produto_embutido} a partir das regras de 'oferta'
    cujo action.type == 'adicionar_brindes'. Se houver lista de brindes, pega o primeiro.
    """
    mapa: dict[str, str] = {}
    for r in cfg.get("rules", []):
        if (r.get("applies_to") or "").lower() != "oferta":
            continue
        action = r.get("action") or {}
        if (action.get("type") or "").lower() != "adicionar_brindes":
            continue
        oferta = r.get("oferta") or {}
        oferta_id = str(oferta.get("oferta_id") or oferta.get("id") or "").strip()
        if not oferta_id:
            continue
        brindes = action.get("brindes") or []
        nome = None
        if isinstance(brindes, list) and brindes:
            b0 = brindes[0]
            if isinstance(b0, str):
                nome = b0.strip()
            elif isinstance(b0, dict):
                nome = str(b0.get("nome") or b0.get("name") or "").strip()
        if nome:
            mapa[oferta_id] = nome
    return mapa

def montar_mapas_cupons(cfg: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    """
    Retorna:
      - cupons_cdf:     {cupom_lower: box}  -> Anual, 2 anos/Bianual, 3 anos/Trianual (independente da recorrência)
      - cupons_bi_mens: {cupom_lower: box}  -> Bimestral, Mensal (independente da recorrência)
    Considera apenas regras de cupom com action.type == 'alterar_box'.
    """
    cupons_cdf: dict[str, str] = {}
    cupons_bi_mens: dict[str, str] = {}

    for r in cfg.get("rules", []):
        if (r.get("applies_to") or "").lower() != "cupom":
            continue
        action = r.get("action") or {}
        if (action.get("type") or "").lower() != "alterar_box":
            continue

        cupom = ((r.get("cupom") or {}).get("nome") or "").strip().lower()
        box = (action.get("box") or "").strip()
        if not cupom or not box:
            continue

        assinaturas = r.get("assinaturas") or []
        if isinstance(assinaturas, str):
            assinaturas = [assinaturas]
        txt = " | ".join(str(x) for x in assinaturas).lower()

        # --- Grupo CDF (plano): Anual / 2 anos / 3 anos
        if ("anual" in txt) or ("2 anos" in txt) or ("bianual" in txt) or ("3 anos" in txt) or ("trianual" in txt):
            cupons_cdf[cupom] = box

        # --- Grupo Bimestral (plano): Bimestral / Mensal
        if ("bimestral" in txt) or ("mensal" in txt):
            cupons_bi_mens[cupom] = box

    return cupons_cdf, cupons_bi_mens

# ---- Schemas do endpoint (sem skus/rules no input) ----
class BuscarAssinaturasIn(BaseModel):
    ano: int = Field(..., ge=1900, le=2100)
    mes: int = Field(..., ge=1, le=12)
    modo_periodo: str                      # "PERÍODO" | "TODAS" (aceita "PERIODO")
    box_nome: Optional[str] = None
    periodicidade: str                     # "mensal" | "bimestral"

class ColetaOut(BaseModel):
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]

@router.post("/coletar", response_model=ColetaOut)
def coletar_assinaturas(in_: BuscarAssinaturasIn) -> ColetaOut:
    try:
        # 1) carrega arquivos
        skus_info = carregar_skus()
        cfg = carregar_cfg()
        ofertas_embutidas = montar_ofertas_embutidas(cfg)
        cupons_cdf, cupons_bi_mens = montar_mapas_cupons(cfg)

        # 2) monta payload base
        dados = montar_payload_busca_assinaturas(
            ano=in_.ano,
            mes=in_.mes,
            modo_periodo=in_.modo_periodo,
            box_nome=in_.box_nome,
            periodicidade=in_.periodicidade,
            skus_info=skus_info,
            # rules_path=str(CFG_PATH),  # opcional agora, pois vamos injetar 'rules' diretamente
        )

        # 3) injeta tudo que o service usa
        dados["rules"] = _normalizar_rules(cfg)                 # regras no payload
        dados["ofertas_embutidas"] = ofertas_embutidas
        dados["cupons_personalizados_cdf"] = cupons_cdf
        dados["cupons_personalizados_bi_mens"] = cupons_bi_mens

        # (compat — se ainda houver uso antigo em algum ponto do código)
        dados["cupons_personalizados_anual"] = cupons_cdf
        dados["cupons_personalizados_bimestral"] = cupons_bi_mens

        # 4) coleta e processa
        transacoes, _reservado, dados2 = gerenciar_coleta_vendas_assinaturas(
            dados=dados,
            skus_info=skus_info,
        )
        linhas, contagem = montar_planilha_vendas_guru(
            transacoes=transacoes,
            dados=dados2,
            skus_info=skus_info,
            atualizar_etapa=None,
        )
        return ColetaOut(linhas=linhas, contagem=contagem)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta: {e!s}")
