from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---- Schema de entrada usado para construir o payload do worker ----
class BuscarAssinaturasIn(BaseModel):
    ano: int = Field(..., ge=1900, le=2100, description="Ano do período (YYYY)")
    mes: int = Field(..., ge=1, le=12, description="Mês do período (1-12)")
    # Numérico na API (0 ou 1); a gente converte para string antes de chamar o worker
    modo_periodo: Literal[0, 1] = Field(..., description="1 = AQUISIÇÕES, 0 = TODAS")
    box_nome: str = Field(..., description="Nome do box (derivado do SKU informado na rota)")
    periodicidade: Literal["mensal", "bimestral"] = Field(..., description='"mensal" ou "bimestral"')
    skus_info: Mapping[str, Mapping[str, Any]] | None = Field(None, description="Mapa de SKUs carregado de skus.json")


class ColetaOut(BaseModel):  # mantém o schema de saída
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]
