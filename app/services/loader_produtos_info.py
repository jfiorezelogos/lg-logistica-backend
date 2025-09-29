from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, TypedDict, cast

import unidecode


# ----------------------------
# Tipos
# ----------------------------
class SKUInfo(TypedDict, total=False):
    sku: str
    peso: float | int
    periodicidade: str
    recorrencia: str
    guru_ids: Sequence[str]
    shopify_ids: Sequence[int | str]
    tipo: str
    indisponivel: bool
    preco_fallback: float


SKUInfoMapping = Mapping[str, Any]
SKUs = Mapping[str, SKUInfoMapping]


# ----------------------------
# Caminho padrão do skus.json
# (repo_root/skus.json)
# ----------------------------
def _default_skus_path() -> Path:
    # este arquivo costuma estar em app/utils/
    # parents[2] → raiz do repo
    repo_root = Path(__file__).resolve().parents[2]
    return repo_root / "skus.json"


# ----------------------------
# Loader com/sem cache
# ----------------------------
def load_skus_info(path: str | Path | None = None, *, create_if_missing: bool = True) -> SKUs:
    """
    Carrega o dicionário de SKUs a partir de `skus.json`.
    Se não existir e create_if_missing=True, cria com um exemplo mínimo.
    Não é cacheado (use `load_skus()` para versão cacheada).
    """
    p = Path(path) if path is not None else _default_skus_path()

    if p.exists():
        with p.open(encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            raise ValueError("skus.json inválido (raiz não é objeto)")
        return cast(SKUs, data)

    if not create_if_missing:
        # retorna vazio se não for para criar
        return cast(SKUs, {})

    p.parent.mkdir(parents=True, exist_ok=True)
    skus_info: dict[str, Any] = {
        "Exemplo Produto": {"sku": "X001", "peso": 1.0, "tipo": "produto", "guru_ids": []},
    }
    with p.open("w", encoding="utf-8") as f:
        json.dump(skus_info, f, indent=2, ensure_ascii=False)
    return cast(SKUs, skus_info)


# cacheia o conteúdo do caminho padrão (útil para rotas que leem sempre o mesmo arquivo)
@lru_cache(maxsize=1)
def load_skus() -> SKUs:
    return load_skus_info(_default_skus_path(), create_if_missing=True)


# ----------------------------
# Busca e normalização
# ----------------------------
def _find_info_by_name(nome: str, skus: SKUs) -> SKUInfoMapping | None:
    if not nome:
        return None
    alvo = unidecode.unidecode(str(nome)).lower().strip()

    # match direto
    info = skus.get(nome)
    if isinstance(info, Mapping):
        return info

    # match normalizado
    for k, i in skus.items():
        if unidecode.unidecode(k).lower().strip() == alvo:
            return i
    return None


def _find_info_by_sku(sku: str, skus: SKUs) -> SKUInfoMapping | None:
    if not sku:
        return None
    sku_norm = str(sku).strip().upper()
    for i in skus.values():
        if isinstance(i, Mapping) and str(i.get("sku", "")).strip().upper() == sku_norm:
            return i
    return None


# ----------------------------
# API pública
# ----------------------------
def get_produto_info(
    nome_ou_sku: str,
    *,
    skus_info: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """
    Aceita o NOME do produto OU o SKU. Tenta primeiro por nome; se não encontrar, tenta por SKU.
    """
    skus = cast(SKUs, skus_info) if skus_info is not None else load_skus()

    # 1) tenta por nome (com normalização)
    info = _find_info_by_name(nome_ou_sku, skus)
    if isinstance(info, Mapping):
        return cast(dict[str, Any], info)

    # 2) tenta por SKU
    info = _find_info_by_sku(nome_ou_sku, skus)
    return cast(dict[str, Any] | None, info if isinstance(info, Mapping) else None)


def get_sku(
    nome: str,
    *,
    skus_info: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    info = get_produto_info(nome, skus_info=skus_info)
    return str((info or {}).get("sku", "")).strip()


def produto_indisponivel(
    produto_nome: str,
    *,
    skus_info: Mapping[str, Mapping[str, Any]] | None = None,
    sku: str | None = None,
) -> bool:
    """
    Verifica se um produto está indisponível, buscando por nome (normalizado)
    e opcionalmente por sku.
    """
    if not produto_nome and not sku:
        return False

    skus = cast(SKUs, skus_info) if skus_info is not None else load_skus()

    info: Mapping[str, Any] | None = _find_info_by_name(produto_nome, skus) if produto_nome else None
    if info is None and sku:
        info = _find_info_by_sku(sku, skus)

    return bool(info and info.get("indisponivel", False))


def is_indisponivel(
    nome: str,
    *,
    skus_info: Mapping[str, Mapping[str, Any]] | None = None,
) -> bool:
    return produto_indisponivel(nome, skus_info=skus_info)


__all__ = [
    "SKUInfo",
    "SKUInfoMapping",
    "SKUs",
    "get_produto_info",
    "get_sku",
    "is_indisponivel",
    "load_skus",
    "load_skus_info",
    "produto_indisponivel",
]
