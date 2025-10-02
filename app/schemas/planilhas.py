from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CreatePlanilhaRequest(BaseModel):
    planilha_id: str = Field(
        ...,
        description="ID da planilha a ser criada (ex.: pln_20251002_154522_ab12cd)",
        pattern=r"^pln_\d{8}_\d{6}_[0-9a-fA-F]{8}$",
        examples=["pln_20251002_154522_ab12cd"],
    )
    meta: dict[str, Any] | None = Field(
        default=None,
        description="Metadados opcionais para salvar junto (ex.: origem, observações)",
        examples=[{"origem": "shopify/pedidos", "observacoes": "coleta matutina"}],
    )


class CreatePlanilhaResponse(BaseModel):
    planilha_id: str = Field(
        ...,
        description="Identificador da planilha criada",
        examples=["pln_20251002_154522_ab12cd"],
    )
    created_at: str = Field(
        ...,
        description="Data/hora local (America/Sao_Paulo) de criação (YYYY-MM-DD HH:MM:SS)",
        examples=["2025-10-02 15:47:22"],
    )
