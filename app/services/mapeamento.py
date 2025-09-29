from collections.abc import Mapping, Sequence
from typing import Any, TypedDict

import unidecode


class SKUInfo(TypedDict, total=False):
    sku: str
    peso: float | int
    periodicidade: str
    guru_ids: Sequence[str]


SKUInfo = Mapping[str, Any]

SKUs = Mapping[str, SKUInfo]


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
        alvo = unidecode(str(produto_nome)).lower().strip()
        for nome, i in skus.items():
            if unidecode(nome).lower().strip() == alvo:
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
