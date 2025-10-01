from __future__ import annotations

import json
import logging
import re
import unicodedata
from collections.abc import Mapping
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def parse_money(val: Any) -> float:
    if pd.isna(val) or str(val).strip() == "":
        return 0.0
    s = str(val).strip().replace(".", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except Exception:
        return 0.0


def limpar(v: Any) -> str:
    return "" if pd.isna(v) else str(v).strip()


# -----------------------------------------------------------------------------
# Helpers gerais
# -----------------------------------------------------------------------------
def _normalizar_order_id(valor: str | int) -> str:
    if isinstance(valor, int):
        return str(valor)
    s = str(valor).strip()
    return s.split("/")[-1] if "gid://" in s and "/" in s else s


def normalizar_texto(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().strip()


def validar_endereco(address1: str) -> bool:
    # heurística: presença de algum dígito costuma indicar número ao lado
    return bool(re.search(r"\d", address1 or ""))


def registrar_log_norm_enderecos(order_id: str, resultado: Mapping[str, Any]) -> None:
    try:
        logger.info(
            "addr_norm_result",
            extra={"order_id": order_id, "resultado": json.dumps(dict(resultado), ensure_ascii=False)},
        )
    except Exception:
        logger.info("addr_norm_result", extra={"order_id": order_id})
