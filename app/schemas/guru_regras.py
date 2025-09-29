# app/schemas/regras.py
from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, model_validator


class CupomCfg(BaseModel):
    nome: str = Field(..., description="Código do cupom")


class OfertaCfg(BaseModel):
    produto_id: str | None = None
    oferta_id: str | None = None
    nome: str | None = None


class ActionAdicionarBrindes(BaseModel):
    type: Literal["adicionar_brindes"] = "adicionar_brindes"
    brindes: list[str | dict[str, Any]]
    box: str | None = None


class ActionAlterarBox(BaseModel):
    type: Literal["alterar_box"] = "alterar_box"
    box: str
    brindes: list[str | dict[str, Any]] | None = None


Action = Annotated[ActionAdicionarBrindes | ActionAlterarBox, Field(discriminator="type")]


class Regra(BaseModel):
    id: UUID | None = None
    applies_to: Literal["cupom", "oferta"]
    enabled: bool = True
    assinaturas: list[str] | None = None
    cupom: CupomCfg | None = None
    oferta: OfertaCfg | None = None
    action: Action

    @model_validator(mode="after")
    def _check_alvos(self) -> Regra:
        if self.applies_to == "cupom":
            if self.cupom is None:
                raise ValueError("Para applies_to='cupom', o bloco 'cupom' é obrigatório.")
            if self.oferta is not None:
                raise ValueError("Para applies_to='cupom', não envie 'oferta'.")
        else:
            if self.oferta is None:
                raise ValueError("Para applies_to='oferta', o bloco 'oferta' é obrigatório.")
            if self.cupom is not None:
                raise ValueError("Para applies_to='oferta', não envie 'cupom'.")
        return self

    @model_validator(mode="after")
    def _ensure_id(self) -> Regra:
        if self.id is None:
            self.id = uuid4()
        return self


class ConfigOfertas(BaseModel):
    rules: list[Regra] = Field(default_factory=list)
