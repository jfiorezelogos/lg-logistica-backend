from pydantic import BaseModel
from typing import Any, Mapping, Optional

class BuscarAssinaturasIn(BaseModel):
    ano: int
    mes: int
    modo_periodo: str      # "PERÍODO" | "TODAS"
    box_nome: Optional[str] = None
    periodicidade: str     # "mensal" | "bimestral"
    skus_info: Optional[Mapping[str, Mapping[str, Any]]] = None