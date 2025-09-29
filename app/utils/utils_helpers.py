from __future__ import annotations

from typing import Any

import pandas as pd


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
