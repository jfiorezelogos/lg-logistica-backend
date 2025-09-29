from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

# agora inclui 'assinatura'
TipoItem = Literal["produto", "combo", "assinatura"]


class ItemCreate(BaseModel):
    """
    Criação de item por SKU (para POST /catalogo/{sku}).
    Diferencia por `tipo`:
      - produto: ignora `composto_de`
      - combo:   exige `composto_de` (lista de SKUs componentes)
      - assinatura: exige `recorrencia` e `periodicidade` ('mensal' | 'bimestral')
    """
    tipo: TipoItem = Field(..., description="Tipo do item: 'produto' | 'combo' | 'assinatura'")
    nome: str = Field(..., description="Nome (chave no arquivo; usado apenas no armazenamento)")
    peso: float = Field(0.0, ge=0, description="Peso em kg (assinatura geralmente 0.0)")
    guru_ids: list[str] = Field(default_factory=list, description="IDs do Guru (lista ou string 'a,b,c')")
    shopify_ids: list[int] = Field(default_factory=list, description="IDs da Shopify (lista ou string '1,2,3')")
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False

    # combo
    composto_de: list[str] = Field(default_factory=list, description="(combo) lista de SKUs componentes")

    # assinatura
    recorrencia: str = Field("", description="(assinatura) anual | bianual | trianual | mensal | bimestral")
    periodicidade: str = Field("", description="(assinatura) 'mensal' | 'bimestral'")

    @field_validator("guru_ids", mode="before")
    @classmethod
    def _norm_guru(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any) -> list[int]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]

    @model_validator(mode="after")
    def _require_fields_by_tipo(self) -> "ItemCreate":
        t = (self.tipo or "").strip().lower()
        if t == "combo" and not self.composto_de:
            raise ValueError("Para combo, 'composto_de' é obrigatório (lista de SKUs).")
        if t == "assinatura":
            per = (self.periodicidade or "").strip().lower()
            if per not in ("mensal", "bimestral"):
                raise ValueError("Para assinatura, 'periodicidade' deve ser 'mensal' ou 'bimestral'.")
            self.recorrencia = (self.recorrencia or "").strip()
            self.periodicidade = per
        return self


class ProdutoIn(BaseModel):
    """
    Item simples do catálogo (não combo, não assinatura).
    A API usa SKU como chave principal.
    """
    nome: str = Field(..., description="Nome do item (chave no arquivo `skus.json`).")
    sku: str = Field(..., min_length=1, description="SKU interno do produto (obrigatório e idealmente único).")
    peso: float = Field(0.0, ge=0, description="Peso em kg.")
    guru_ids: list[str] = Field(default_factory=list, description="IDs do Guru.")
    shopify_ids: list[int] = Field(default_factory=list, description="IDs da Shopify.")
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False

    @field_validator("sku")
    @classmethod
    def _chk_sku_obrigatorio(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("SKU obrigatório")
        return v

    @field_validator("guru_ids", mode="before")
    @classmethod
    def _norm_guru(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any) -> list[int]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]


class AssinaturaIn(BaseModel):
    """
    Item do tipo 'assinatura' COM SKU (alinhado ao novo modelo).
    """
    nome: str = Field(..., description="Nome da assinatura (chave no arquivo).")
    sku: str = Field(..., min_length=1, description="SKU interno da assinatura (obrigatório e idealmente único).")
    recorrencia: str = Field("", description="anual | bianual | trianual | mensal | bimestral")
    periodicidade: str = Field(..., description="'mensal' | 'bimestral'")
    guru_ids: list[str] = Field(default_factory=list, description="IDs do Guru.")
    shopify_ids: list[int] = Field(default_factory=list, description="IDs da Shopify.")
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False
    peso: float = Field(0.0, ge=0, description="Peso em kg (normalmente 0.0)")
    composto_de: list[str] = Field(default_factory=list, description="Mantido por consistência (vazio para assinatura)")

    @field_validator("sku")
    @classmethod
    def _chk_sku_obrigatorio(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("SKU obrigatório")
        return v

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

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any) -> list[int]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]


class ComboIn(BaseModel):
    """
    Combo (conjunto de SKUs). A API trabalha por SKU.
    """
    nome: str = Field(..., description="Nome do combo (chave no arquivo `skus.json`).")
    sku: str = Field(..., min_length=1, description="SKU interno do combo (obrigatório e idealmente único).")
    composto_de: list[str] = Field(default_factory=list, description="Lista de SKUs componentes do combo.")
    guru_ids: list[str] = Field(default_factory=list, description="IDs do Guru.")
    shopify_ids: list[int] = Field(default_factory=list, description="IDs da Shopify.")
    preco_fallback: float | None = Field(None, ge=0)
    indisponivel: bool = False
    peso: float = Field(0.0, ge=0, description="Peso em kg (geralmente 0.0)")

    @field_validator("sku")
    @classmethod
    def _chk_sku_obrigatorio(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("SKU obrigatório")
        return v

    @field_validator("composto_de", "guru_ids", mode="before")
    @classmethod
    def _norm_list(cls, v: Any) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any) -> list[int]:
        if v is None:
            return []
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]


class SKUsPayload(BaseModel):
    """
    Estrutura completa para substituir todo o `skus.json` via PUT `/catalogo/`.
    O arquivo segue nome→info, mas todos os itens (produto/combo/assinatura) carregam SKU.
    """
    skus: dict[str, dict[str, Any]] = Field(default_factory=dict)


# =========================
# PATCHes parciais por SKU
# =========================

class ProdutoPatch(BaseModel):
    """Atualização parcial de um produto (aplica apenas campos enviados)."""
    peso: Optional[float] = Field(None, ge=0)
    guru_ids: Optional[list[str]] = None
    shopify_ids: Optional[list[int]] = None
    preco_fallback: Optional[float] = Field(None, ge=0)
    indisponivel: Optional[bool] = None

    @field_validator("guru_ids", mode="before")
    @classmethod
    def _norm_guru(cls, v: Any):
        if v is None:
            return v
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any):
        if v is None:
            return v
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]


class AssinaturaPatch(BaseModel):
    """Atualização parcial de uma assinatura (aplica apenas campos enviados)."""
    recorrencia: Optional[str] = Field(None, description="anual | bianual | trianual | mensal | bimestral")
    periodicidade: Optional[str] = Field(None, description="'mensal' | 'bimestral'")
    guru_ids: Optional[list[str]] = None
    shopify_ids: Optional[list[int]] = None
    preco_fallback: Optional[float] = Field(None, ge=0)
    indisponivel: Optional[bool] = None
    peso: Optional[float] = Field(None, ge=0)

    @field_validator("guru_ids", mode="before")
    @classmethod
    def _norm_guru(cls, v: Any):
        if v is None:
            return v
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any):
        if v is None:
            return v
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]

    @field_validator("periodicidade")
    @classmethod
    def _chk_per(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        per = (v or "").strip().lower()
        if per not in ("mensal", "bimestral"):
            raise ValueError("periodicidade deve ser 'mensal' ou 'bimestral'")
        return per


class ComboPatch(BaseModel):
    """Atualização parcial de um combo (aplica apenas campos enviados)."""
    composto_de: Optional[list[str]] = None
    guru_ids: Optional[list[str]] = None
    shopify_ids: Optional[list[int]] = None
    preco_fallback: Optional[float] = Field(None, ge=0)
    indisponivel: Optional[bool] = None
    peso: Optional[float] = Field(None, ge=0)

    @field_validator("composto_de", "guru_ids", mode="before")
    @classmethod
    def _norm_list(cls, v: Any):
        if v is None:
            return v
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",")]
        return [str(x).strip() for x in list(v) if str(x).strip()]

    @field_validator("shopify_ids", mode="before")
    @classmethod
    def _norm_shopify(cls, v: Any):
        if v is None:
            return v
        if isinstance(v, str):
            v = [x.strip() for x in v.split(",") if x.strip()]
        return [int(x) for x in list(v) if str(x).strip().isdigit()]


# =========================
# Schemas atômicos (add/remove)
# =========================

class IdStrIn(BaseModel):
    id: str = Field(..., min_length=1, description="ID string para adicionar/remover (ex.: Guru ID)")

    @field_validator("id")
    @classmethod
    def _not_empty(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("id vazio")
        return v


class IdIntIn(BaseModel):
    id: int = Field(..., description="ID inteiro para adicionar/remover (ex.: Shopify ID)")