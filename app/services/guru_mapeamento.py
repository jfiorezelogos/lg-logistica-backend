# app/services/guru_mapeamento.py
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TypedDict, cast

from app.services.loader_produtos_info import load_skus

SKUS_PATH = Path(__file__).resolve().parents[2] / "skus.json"


class MapearIn(TypedDict, total=False):
    sku: str
    tipo: str  # "produto" | "assinatura" | "combo"
    guru_ids: Sequence[str]
    recorrencia: str | None  # (assinatura) anual | bianual | trianual
    periodicidade: str | None  # (assinatura) mensal | bimestral


class MapearOut(TypedDict, total=False):
    sku: str
    tipo: str
    guru_ids: list[str]
    recorrencia: str | None
    periodicidade: str | None
    message: str


def _write_json_atomic(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp, path)  # atomic on Windows/Unix
    finally:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def mapear_produtos_guru(req: MapearIn) -> MapearOut:
    sku = (req.get("sku") or "").strip()
    tipo = (req.get("tipo") or "").strip().lower()
    guru_ids_in = [str(g).strip() for g in (req.get("guru_ids") or []) if str(g).strip()]

    if not sku:
        raise ValueError("SKU é obrigatório.")
    if tipo not in {"produto", "assinatura", "combo"}:
        raise ValueError("tipo deve ser 'produto', 'assinatura' ou 'combo'.")

    if tipo == "assinatura":
        if not (req.get("recorrencia") and req.get("periodicidade")):
            raise ValueError("Para 'assinatura', informe 'recorrencia' e 'periodicidade'.")

    skus_info: dict[str, Any] = dict(load_skus())  # snapshot em memória

    # cria/obtém entrada
    entrada: dict[str, Any] = cast(dict[str, Any], skus_info.setdefault(sku, {}))

    # atualiza tipo e metadados
    if tipo == "assinatura":
        entrada["tipo"] = "assinatura"
        entrada["recorrencia"] = req.get("recorrencia")
        entrada["periodicidade"] = req.get("periodicidade")
        entrada.setdefault("sku", sku)
        entrada.setdefault("peso", 0.0)
        entrada.setdefault("composto_de", [])
    elif tipo == "combo":
        entrada["tipo"] = "combo"
        entrada.pop("recorrencia", None)
        entrada.pop("periodicidade", None)
    else:
        entrada["tipo"] = "produto"
        entrada.pop("recorrencia", None)
        entrada.pop("periodicidade", None)

    # atualiza guru_ids sem duplicar
    entrada.setdefault("guru_ids", [])
    ja = {str(x).strip() for x in entrada["guru_ids"]}
    for gid in guru_ids_in:
        if gid not in ja:
            entrada["guru_ids"].append(gid)
            ja.add(gid)

    _write_json_atomic(SKUS_PATH, skus_info)

    return {
        "sku": sku,
        "tipo": entrada["tipo"],
        "guru_ids": list(entrada["guru_ids"]),
        "recorrencia": entrada.get("recorrencia"),
        "periodicidade": entrada.get("periodicidade"),
        "message": f"SKU '{sku}' mapeado com sucesso!",
    }
