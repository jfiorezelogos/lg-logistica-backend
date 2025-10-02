from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---- Schema de entrada usado para construir o payload do worker ----
class BuscarAssinaturasIn(BaseModel):
    ano: int = Field(..., ge=1900, le=2100, description="Ano do período (YYYY)")
    mes: int = Field(..., ge=1, le=12, description="Mês do período (1-12)")
    # Numérico na API (0 ou 1); a rota converte para string antes de chamar o worker.
    modo_periodo: Literal[0, 1] = Field(
        ...,
        description="Modo do período: 1 = PERÍODO, 0 = TODAS",
        examples=[1],
    )
    box_nome: str = Field(..., description="Nome do box (derivado do SKU informado na rota)")
    periodicidade: Literal["mensal", "bimestral"] = Field(..., description='"mensal" ou "bimestral"')
    skus_info: Mapping[str, Mapping[str, Any]] | None = Field(None, description="Mapa de SKUs carregado de skus.json")

    # Helper: converte 0/1 para a string esperada pelo worker
    def modo_periodo_str(self) -> str:
        return "PERÍODO" if self.modo_periodo == 1 else "TODAS"


# ---- Bloco de persistência (obrigatório, já que a rota exige planilha_id) ----
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
            "- Linha principal: `transaction_id`.\n"
            "- Linhas derivadas (combo, brinde por cupom, embutido por oferta): `transaction_id+SKU`."
        ),
        examples=[{"planilha_id": "pln_20251002_154522_ab12cd", "adicionados": 120, "atualizados": 8}],
    )
