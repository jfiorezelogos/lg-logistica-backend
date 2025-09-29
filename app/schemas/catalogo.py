from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator


class ProdutoIn(BaseModel):
    nome: str = Field(..., description="Nome do item no catálogo (chave)")
    sku: str = Field("", description="SKU interno")
    peso: float = Field(0.0, ge=0, description="Peso em kg")
    guru_ids: list[str] = Field(default_factory=list)
    shopify_ids: list[int] = Field(default_factory=list)
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False

    @field_validator("guru_ids", mode="before")
    @classmethod
    def _norm_guru(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]


class AssinaturaIn(BaseModel):
    nome_base: str = Field(..., description="Nome base da assinatura (sem periodicidade)")
    recorrencia: str = Field("", description="Ex.: anual, bianual, trianual, mensal, bimestral")
    periodicidade: str = Field(..., description="mensal | bimestral")
    guru_ids: list[str] = Field(default_factory=list)
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False

    @field_validator("periodicidade")
    @classmethod
    def _chk_periodicidade(cls, v: str) -> str:
        v = (v or "").strip().lower()
        return v if v in ("mensal", "bimestral") else "bimestral"

    @field_validator("guru_ids", mode="before")
    @classmethod
    def _norm_guru(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]


class ComboIn(BaseModel):
    nome: str = Field(..., description="Nome do combo (chave)")
    sku: str = Field("", description="SKU interno do combo")
    composto_de: list[str] = Field(default_factory=list, description="Lista de SKUs de itens do combo")
    guru_ids: list[str] = Field(default_factory=list)
    shopify_ids: list[int] = Field(default_factory=list)
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False

    @field_validator("composto_de", "guru_ids", mode="before")
    @classmethod
    def _norm_list(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]


class SKUsPayload(BaseModel):
    """
    Estrutura completa para substituir todo o skus.json.
    """

    skus: dict[str, dict[str, Any]] = Field(default_factory=dict)
