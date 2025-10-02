from __future__ import annotations

from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field


class TransportadoraEnum(str, Enum):
    CORREIOS = "CORREIOS"
    GFL = "GFL"
    GOL = "GOL"
    JET = "JET"
    LOG = "LOG"


class EntradaIdentificacao(BaseModel):
    email: str = Field(..., description="E-mail do comprador (usado para chave do lote)")
    cep: str = Field(..., description="CEP de entrega (com ou sem máscara)")
    numero_entrega: str = Field(..., description="Número do endereço de entrega (ex.: '1500' ou '221A')")


# app/schemas/fretebarato_cotacao.py (acréscimo campo)
class CotarFretesAutoRequest(BaseModel):
    planilha_id: str = Field(..., description="Planilha-base para ler o snapshot de pedidos")
    selecionadas: Annotated[list[TransportadoraEnum], Field(min_length=1)] = ...
    entradas: Annotated[list[EntradaIdentificacao], Field(min_length=1)] = ...
    incluir_todas_cotacoes: bool = False


class CotacaoOp(BaseModel):
    nome_transportadora: TransportadoraEnum
    nome_servico: str | None = None
    valor: float


class ResultadoLote(BaseModel):
    id_lote: str
    email: str
    cep: str
    melhor: CotacaoOp | None
    todas: list[CotacaoOp] = []
    mensagem: str | None = None
    valor_total: float | None = Field(None, description="Soma usada na cotação")
    peso_total: float | None = Field(None, description="Soma (kg) usada na cotação")


class CotarFretesResponse(BaseModel):
    ok: bool
    resultados: list[ResultadoLote]
    total_lotes: int
    total_com_frete: int
