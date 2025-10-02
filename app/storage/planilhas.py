from __future__ import annotations

import contextlib
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_BASE = Path("var/planilhas")
_BASE.mkdir(parents=True, exist_ok=True)

TZ_BR = ZoneInfo("America/Sao_Paulo")


def _now_local() -> str:
    return datetime.now(TZ_BR).strftime("%Y-%m-%d %H:%M:%S")


def _path(planilha_id: str) -> Path:
    return _BASE / f"{planilha_id}.json"


def create_planilha(planilha_id: str, meta: dict[str, Any] | None = None) -> str:
    """
    Cria uma planilha vazia em JSON com o id informado.
    Lança FileExistsError se já existir.
    """
    p = _path(planilha_id)
    if p.exists():
        raise FileExistsError(f"planilha {planilha_id} já existe")

    payload = {
        "planilha_id": planilha_id,
        "version": 1,
        "created_at": _now_local(),
        "updated_at": _now_local(),
        "row_count": 0,
        "meta": meta or {},
        "lines": [],
        # índice persistido (lista) para dedupe rápido
        "index": {"dedup_ids": []},
    }
    save_planilha(planilha_id, payload)
    return planilha_id


def save_planilha(planilha_id: str, payload: dict[str, Any]) -> Path:
    p = _path(planilha_id)
    tmp_fd, tmp_path = tempfile.mkstemp(prefix=f"{planilha_id}.", suffix=".tmp", dir=_BASE)
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, p)
    except Exception:
        with contextlib.suppress(Exception):
            os.remove(tmp_path)
        raise
    return p


def load_planilha(planilha_id: str) -> dict[str, Any]:
    p = _path(planilha_id)
    if not p.exists():
        raise FileNotFoundError(f"planilha_id não encontrada: {planilha_id}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("lines", [])
    data.setdefault("index", {}).setdefault("dedup_ids", [])
    return data


# --------- Helpers de dedupe ---------
def _infer_dedup_id(row: dict[str, Any]) -> str:
    """
    Retorna o ID canônico por linha:
      - Prioridade Shopify: id_line_item / line_item_id
      - Fallback: transaction_id (Guru já é por linha)
      - Se nada for encontrado, retorna ""
    """
    for key in ("dedup_id", "id_line_item", "line_item_id", "transaction_id"):
        val = row.get(key)
        if val is None:
            continue
        s = str(val).strip()
        if s:
            return s
    return ""


def _ensure_dedup_id_inplace(row: dict[str, Any]) -> str:
    """
    Garante que a linha tenha 'dedup_id' preenchido (in-place),
    inferindo a partir de id_line_item/line_item_id/transaction_id quando necessário.
    """
    did = str(row.get("dedup_id") or "").strip()
    if did:
        return did
    did = _infer_dedup_id(row)
    if did:
        row["dedup_id"] = did
    return did


# --------- Append com merge por dedup_id ---------
def append_coleta(planilha_id: str, novas_linhas: list[dict[str, Any]]) -> tuple[int, int]:
    """
    Acrescenta/atualiza linhas deduplicando por 'dedup_id' (ID único por linha).
    - Se 'dedup_id' (ou equivalente) não existe ainda: adiciona a linha.
    - Se já existe: faz update() na linha existente (enriquecendo/atualizando), sem criar nova linha.
    Retorna (adicionados, atualizados).
    """
    data = load_planilha(planilha_id)

    # índice in-memory: dedup_id -> referência da linha existente
    idx: dict[str, dict[str, Any]] = {}
    for row in data.get("lines", []):
        did = _ensure_dedup_id_inplace(row)
        if did:
            idx[did] = row

    adicionados, atualizados = 0, 0

    for row in novas_linhas:
        did = _ensure_dedup_id_inplace(row)
        if not did:
            # Sem dedup_id identificável: adiciona assim mesmo
            data["lines"].append(row)
            adicionados += 1
            continue

        if did in idx:
            # Atualiza/enriquece a linha existente
            idx[did].update(row)
            atualizados += 1
        else:
            data["lines"].append(row)
            idx[did] = row
            adicionados += 1

    data["row_count"] = len(data["lines"])
    data["updated_at"] = _now_local()
    data.setdefault("index", {})["dedup_ids"] = sorted(idx.keys())

    save_planilha(planilha_id, data)
    return adicionados, atualizados
