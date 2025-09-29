from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel


class BuscarAssinaturasIn(BaseModel):
    ano: int
    mes: int
    modo_periodo: str  # "PERÍODO" | "TODAS"
    box_nome: str | None = None
    periodicidade: str  # "mensal" | "bimestral"
    skus_info: Mapping[str, Mapping[str, Any]] | None = None
