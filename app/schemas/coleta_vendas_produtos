from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any, Optional

from pydantic import BaseModel, Field


class BuscarProdutosIn(BaseModel):
    data_ini: date = Field(..., description="Data inicial no formato YYYY-MM-DD")
    data_fim: date = Field(..., description="Data final no formato YYYY-MM-DD")
    nome_produto: Optional[str] = Field(None, description="Nome do produto ou None para todos")
    skus_info: Optional[Mapping[str, Mapping[str, Any]]] = None
