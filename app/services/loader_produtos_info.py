from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TypedDict, cast

import unidecode


# ---------- Tipos ----------
class SKUInfo(TypedDict, total=False):
    sku: str
    peso: float | int
    periodicidade: str
    guru_ids: Sequence[str]
    tipo: str
    indisponivel: bool


SKUInfoMapping = Mapping[str, Any]
SKUs = Mapping[str, SKUInfoMapping]


# ---------- Funções ----------
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


def load_skus_info(path: str | Path | None = None) -> SKUs:
    """
    Carrega o dicionário de SKUs a partir de `skus.json`.
    Se não existir, cria com um exemplo mínimo.
    """
    if path is None:
        # raiz do projeto (ajuste se precisar)
        base_dir = Path(__file__).resolve().parents[2]
        path = base_dir / "skus.json"
    p = Path(path)

    if p.exists():
        with p.open(encoding="utf-8") as f:
            return cast(SKUs, json.load(f))

    # fallback se ainda não existir
    skus_info: dict[str, Any] = {
        "Exemplo Produto": {"sku": "X001", "peso": 1.0, "tipo": "produto", "guru_ids": []},
    }
    with p.open("w", encoding="utf-8") as f:
        json.dump(skus_info, f, indent=4, ensure_ascii=False)
    return skus_info


__all__ = ["SKUInfo", "SKUInfoMapping", "SKUs", "load_skus_info", "produto_indisponivel"]
