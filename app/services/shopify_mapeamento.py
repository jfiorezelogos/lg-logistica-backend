from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, cast

from app.services.loader_produtos_info import load_skus

SKUS_PATH = Path(__file__).resolve().parents[2] / "skus.json"


def _write_json_atomic(path: Path, data: Mapping[str, Any]) -> None:
    """Grava JSON de forma atômica, evitando corrupção em caso de falha."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def mapear_produtos_shopify_service(
    sku: str,
    shopify_ids_in: Sequence[int | str],
) -> dict[str, Any]:
    """
    Atualiza o skus.json adicionando IDs da Shopify ao SKU informado.
    - Não duplica IDs
    - Converte para int quando possível
    """
    sku_norm = (sku or "").strip().upper()
    if not sku_norm:
        raise ValueError("SKU é obrigatório.")

    skus_info: MutableMapping[str, Any] = cast(MutableMapping[str, Any], dict(load_skus()))

    # Localiza entrada pelo SKU
    entrada: MutableMapping[str, Any] | None = None
    for _nome, info in skus_info.items():
        if str(info.get("sku", "")).strip().upper() == sku_norm:
            entrada = cast(MutableMapping[str, Any], info)
            break

    if entrada is None:
        raise ValueError(f"SKU '{sku}' não encontrado no skus.json")

    entrada.setdefault("shopify_ids", [])
    atuais_set = {str(x).strip() for x in entrada["shopify_ids"] if str(x).strip()}

    novos_normalizados: list[int | str] = []
    for sid in shopify_ids_in:
        s = str(sid).strip()
        if not s or s in atuais_set:
            continue
        try:
            val: int | str = int(s)
        except Exception:
            val = s
        novos_normalizados.append(val)
        atuais_set.add(s)

    if novos_normalizados:
        entrada["shopify_ids"].extend(novos_normalizados)
        _write_json_atomic(SKUS_PATH, skus_info)
        return {
            "sku": sku_norm,
            "shopify_ids": list(entrada["shopify_ids"]),
            "adicionados": len(novos_normalizados),
            "total_mapeados": len(entrada["shopify_ids"]),
            "message": f"Mapeados {len(novos_normalizados)} ID(s) da Shopify para '{sku_norm}'.",
        }
    else:
        _write_json_atomic(SKUS_PATH, skus_info)
        return {
            "sku": sku_norm,
            "shopify_ids": list(entrada["shopify_ids"]),
            "adicionados": 0,
            "total_mapeados": len(entrada["shopify_ids"]),
            "message": "Nenhum ID novo para mapear.",
        }
