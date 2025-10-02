from __future__ import annotations

import logging
import unicodedata
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Helpers gerais
# -----------------------------------------------------------------------------
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


def normalizar_order_id(valor: str | int) -> str:
    if isinstance(valor, int):
        return str(valor)
    s = str(valor).strip()
    return s.split("/")[-1] if "gid://" in s and "/" in s else s


def normalizar_texto(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.lower().strip()
