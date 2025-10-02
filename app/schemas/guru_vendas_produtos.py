from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from typing import Any

from pydantic import BaseModel, Field


class BuscarProdutosIn(BaseModel):
    data_ini: date = Field(..., description="Data inicial no formato YYYY-MM-DD")
    data_fim: date = Field(..., description="Data final no formato YYYY-MM-DD")
    nome_produto: str | None = Field(None, description="Nome do produto ou None para todos")
    skus_info: Mapping[str, Mapping[str, Any]] | None = None


class PersistenciaPlanilha(BaseModel):
    planilha_id: str = Field(..., description="Identificador da planilha de destino")
    adicionados: int = Field(..., ge=0, description="Linhas novas adicionadas")
    atualizados: int = Field(..., ge=0, description="Linhas existentes que foram enriquecidas/atualizadas")


class ColetaOut(BaseModel):
    linhas: list[dict[str, Any]]
    contagem: dict[str, dict[str, int]]
    persistencia: PersistenciaPlanilha = Field(
        ...,
        description=(
            "Resumo da gravação em planilha (planilha_id, adicionados, atualizados).\n\n"
            "🔹 **Regras de deduplicação (dedup_id obrigatório)**:\n"
            "- **Linha principal (produto)**: `transaction_id`.\n"
            "- **Itens de combo**: `transaction_id+SKU`."
        ),
        examples=[{
            "planilha_id": "pln_20251002_154522_ab12cd",
            "adicionados": 95,
            "atualizados": 12
        }],
    )
