from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from typing import Any, TypedDict, cast

import unidecode


class SKUInfo(TypedDict, total=False):
    sku: str
    peso: float | int
    periodicidade: str
    guru_ids: Sequence[str]
    tipo: str
    indisponivel: bool


SKUInfoMapping = Mapping[str, Any]
SKUs = Mapping[str, SKUInfoMapping]


def produto_indisponivel(
    produto_nome: str,
    *,
    skus_info: Mapping[str, Mapping[str, Any]] | None = None,
    sku: str | None = None,
) -> bool:
    if not produto_nome and not sku:
        return False

    skus: Mapping[str, Mapping[str, Any]] = skus_info or {}
    info: Mapping[str, Any] | None = skus.get(produto_nome)

    # fallback por normalização do nome
    if info is None and produto_nome:
        alvo = unidecode.unidecode(str(produto_nome)).lower().strip()
        for nome, i in skus.items():
            if unidecode.unidecode(nome).lower().strip() == alvo:
                info = i
                break

    # fallback por SKU
    if info is None and sku:
        sku_norm = (sku or "").strip().upper()
        for i in skus.values():
            if str(i.get("sku", "")).strip().upper() == sku_norm:
                info = i
                break

    return bool(info and info.get("indisponivel", False))


# 🔹 helper centralizado para carregar o skus.json
def load_skus_info(path: str | None = None) -> SKUs:
    """
    Carrega o dicionário de SKUs a partir de `skus.json`.
    Se não existir, cria com alguns exemplos mínimos.
    """
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "..", "skus.json")

    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return cast(SKUs, json.load(f))

    # fallback se ainda não existir
    skus_info: dict[str, Any] = {
        "Exemplo Produto": {"sku": "X001", "peso": 1.0, "tipo": "produto", "guru_ids": []},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(skus_info, f, indent=4, ensure_ascii=False)
    return skus_info
