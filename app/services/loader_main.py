# app/services/loaders/loader.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from app.infra.cache import simple_cache

# reexports (tipos e utilitários de domínio)
from .loader_produtos_info import (
    SKUInfo,
    SKUInfoMapping,
    SKUs,
    load_skus_info,  # opcional: mantém também o loader “local” por caminho
    produto_indisponivel,
)
from .loader_regras_assinaturas import (
    montar_mapas_cupons,
    montar_ofertas_embutidas,
    normalizar_rules,
)

# ---------- caminhos padrão de arquivos ----------
BASE_DIR = Path(__file__).resolve().parents[2]
SKUS_PATH = BASE_DIR / "skus.json"
CFG_PATH = BASE_DIR / "config_ofertas.json"


# ---------- loaders com cache ----------
@simple_cache(maxsize=1)
def carregar_skus() -> dict[str, dict[str, Any]]:
    try:
        with SKUS_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"Arquivo não encontrado: {SKUS_PATH}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao ler {SKUS_PATH.name}: {e!s}")


@simple_cache(maxsize=1)
def carregar_cfg() -> dict[str, Any]:
    try:
        with CFG_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        raise HTTPException(status_code=500, detail=f"Arquivo não encontrado: {CFG_PATH}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao ler {CFG_PATH.name}: {e!s}")


# ---------- utilitários (opcionais) ----------
def invalidar_cache_catalogo() -> None:
    """Permite dar cache-bust quando os arquivos forem atualizados em runtime."""
    try:
        carregar_skus.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        carregar_cfg.cache_clear()  # type: ignore[attr-defined]
    except Exception:
        pass


__all__ = [
    # tipos
    "SKUInfo",
    "SKUInfoMapping",
    "SKUs",
    # domínio
    "produto_indisponivel",
    "normalizar_rules",
    "montar_ofertas_embutidas",
    "montar_mapas_cupons",
    # loaders
    "carregar_skus",
    "carregar_cfg",
    "invalidar_cache_catalogo",
    # opcional
    "load_skus_info",
]
