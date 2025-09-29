from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

BASE_DIR = Path(__file__).resolve().parents[2]
SKUS_PATH = BASE_DIR / "skus.json"


def gerar_chave_assinatura(nome: str, periodicidade: str) -> str:
    """
    Gera a chave única de uma assinatura no formato "<nome> - <periodicidade>",
    com fallback seguro em 'bimestral'.
    """
    p = (periodicidade or "").strip().lower()
    if p not in ("mensal", "bimestral"):
        p = "bimestral"
    return f"{nome.strip()} - {p}"


def carregar_skus(path: str | Path = SKUS_PATH) -> dict[str, dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("skus.json inválido: conteúdo não é objeto")
    # força dict[str, dict]
    out: dict[str, dict[str, Any]] = {}
    for k, v in data.items():
        if isinstance(v, Mapping):
            out[str(k)] = dict(v)
    return out


def salvar_skus(skus: Mapping[str, Mapping[str, Any]], path: str | Path = SKUS_PATH) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(skus, f, indent=4, ensure_ascii=False)
        tmp.replace(p)
    finally:
        if tmp.exists():
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
