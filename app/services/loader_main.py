# app/services/loader_main.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

# Reexports do domínio (NÃO duplicar aqui)
from app.services.loader_produtos_info import (
    SKUInfo,
    SKUInfoMapping,
    SKUs,
    produto_indisponivel,
    load_skus_info,
)
from app.services.loader_regras_assinaturas import (
    normalizar_rules,
    montar_ofertas_embutidas,
    montar_mapas_cupons,
)

# Cache simples (pode trocar a implementação em infra/cache.py depois)
try:
    from app.infra.cache import simple_cache
except Exception:  # fallback sem infra
    from functools import lru_cache as simple_cache  # type: ignore


# ------------------------- caminhos padrão -------------------------
BASE_DIR = Path(__file__).resolve().parents[2]  # raiz do projeto
SKUS_PATH = BASE_DIR / "skus.json"
CFG_PATH = BASE_DIR / "config_ofertas.json"


# ------------------------- loaders de arquivo -------------------------
@simple_cache(maxsize=1)
def carregar_skus() -> dict[str, dict[str, Any]]:
    """
    Carrega o skus.json da raiz do projeto.
    Retorna dict[str, dict] (mesmo shape que você usa no app).
    """
    try:
        with SKUS_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"Arquivo não encontrado: {SKUS_PATH}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao ler {SKUS_PATH.name}: {e!s}") from e


@simple_cache(maxsize=1)
def carregar_cfg() -> dict[str, Any]:
    """
    Carrega o config_ofertas.json da raiz do projeto.
    Retorna dict com a chave "rules" (quando existir) + outros metadados que você guardar.
    """
    try:
        with CFG_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"Arquivo não encontrado: {CFG_PATH}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao ler {CFG_PATH.name}: {e!s}") from e


# ------------------------- utilitário de cache-bust -------------------------
def invalidar_cache_catalogo() -> None:
    """
    Invalida os caches de carregar_skus/carregar_cfg quando os arquivos forem atualizados em runtime.
    """
    try:
        carregar_skus.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        carregar_cfg.cache_clear()   # type: ignore[attr-defined]
    except Exception:
        pass


__all__ = [
    # tipos
    "SKUInfo", "SKUInfoMapping", "SKUs",
    # loaders de arquivo
    "carregar_skus", "carregar_cfg", "invalidar_cache_catalogo",
    # domínio (reexports)
    "produto_indisponivel", "load_skus_info",
    "normalizar_rules", "montar_ofertas_embutidas", "montar_mapas_cupons",
    # paths (se quiser usar fora)
    "BASE_DIR", "SKUS_PATH", "CFG_PATH",
]
