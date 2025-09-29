# app/schemas/guru_importacao.py
from __future__ import annotations

from fastapi import Form
from pydantic import BaseModel, Field, model_validator
from pydantic.config import ConfigDict


# -------------------------
# Parâmetros da rota
# -------------------------
class ImportacaoParams(BaseModel):
    sku: str = Field(..., description="SKU do produto conforme skus.json")

    @classmethod
    def as_form(
        cls,
        sku: str = Form(..., description="SKU do produto conforme skus.json"),
    ) -> ImportacaoParams:
        return cls(sku=sku)


# -------------------------
# Linha da planilha do Guru
# -------------------------
class GuruPedidoRow(BaseModel):
    # Identificação/transação
    id_transacao: str = Field(alias="id transação")

    # Produto
    valor_venda: str = Field(alias="valor venda")
    nome_produto: str = Field(alias="nome produto")
    id_produto: str = Field(alias="id produto")

    # Assinatura (duas grafias possíveis)
    assinatura_codigo: str | None = Field(default="", alias="assinatura código")
    assinatura_codigo_alt: str | None = Field(default=None, alias="assinatura codigo", exclude=True)

    # Contato/entrega
    nome_contato: str = Field(alias="nome contato")
    doc_contato: str = Field(alias="doc contato")
    email_contato: str = Field(alias="email contato")
    logradouro_contato: str = Field(alias="logradouro contato")
    numero_contato: str = Field(alias="número contato")
    complemento_contato: str = Field(alias="complemento contato")
    bairro_contato: str = Field(alias="bairro contato")
    cidade_contato: str = Field(alias="cidade contato")
    estado_contato: str = Field(alias="estado contato")
    cep_contato: str = Field(alias="cep contato")
    telefone_contato: str = Field(alias="telefone contato")

    # Datas e pagamento
    data_pedido: str = Field(alias="data pedido")
    pagamento: str = Field(alias="pagamento")

    @model_validator(mode="after")
    def _coalesce_assinatura_codigo(self) -> GuruPedidoRow:
        prim = (self.assinatura_codigo or "").strip()
        alt = (self.assinatura_codigo_alt or "").strip()
        if not prim and alt:
            object.__setattr__(self, "assinatura_codigo", alt)
        return self

    model_config = ConfigDict(populate_by_name=True)


# -------------------------
# Modelo de saída
# -------------------------
class RegistroImportado(BaseModel):
    Numero_pedido: str = Field(alias="Número pedido")
    Nome_Comprador: str = Field(alias="Nome Comprador")
    Data_Pedido: str = Field(alias="Data Pedido")
    Data: str
    CPF_CNPJ_Comprador: str = Field(alias="CPF/CNPJ Comprador")
    Endereco_Comprador: str = Field(alias="Endereço Comprador")
    Bairro_Comprador: str = Field(alias="Bairro Comprador")
    Numero_Comprador: str = Field(alias="Número Comprador")
    Complemento_Comprador: str = Field(alias="Complemento Comprador")
    CEP_Comprador: str = Field(alias="CEP Comprador")
    Cidade_Comprador: str = Field(alias="Cidade Comprador")
    UF_Comprador: str = Field(alias="UF Comprador")
    Telefone_Comprador: str = Field(alias="Telefone Comprador")
    Celular_Comprador: str = Field(alias="Celular Comprador")
    Email_Comprador: str = Field(alias="E-mail Comprador")
    Produto: str
    SKU: str
    Un: str
    Quantidade: str
    Valor_Unitario: str = Field(alias="Valor Unitário")
    Valor_Total: str = Field(alias="Valor Total")
    Total_Pedido: str = Field(alias="Total Pedido")
    Valor_Frete_Pedido: str = Field(alias="Valor Frete Pedido")
    Valor_Desconto_Pedido: str = Field(alias="Valor Desconto Pedido")
    Outras_despesas: str = Field(alias="Outras despesas")
    Nome_Entrega: str = Field(alias="Nome Entrega")
    Endereco_Entrega: str = Field(alias="Endereço Entrega")
    Numero_Entrega: str = Field(alias="Número Entrega")
    Complemento_Entrega: str = Field(alias="Complemento Entrega")
    Cidade_Entrega: str = Field(alias="Cidade Entrega")
    UF_Entrega: str = Field(alias="UF Entrega")
    CEP_Entrega: str = Field(alias="CEP Entrega")
    Bairro_Entrega: str = Field(alias="Bairro Entrega")
    Transportadora: str
    Servico: str = Field(alias="Serviço")
    Tipo_Frete: str = Field(alias="Tipo Frete")
    Observacoes: str = Field(alias="Observações")
    Qtd_Parcela: str = Field(alias="Qtd Parcela")
    Data_Prevista: str = Field(alias="Data Prevista")
    Vendedor: str
    Forma_Pagamento: str = Field(alias="Forma Pagamento")
    ID_Forma_Pagamento: str = Field(alias="ID Forma Pagamento")
    transaction_id: str
    indisponivel: str
    periodicidade: str
    Plano_Assinatura: str = Field(alias="Plano Assinatura")
    assinatura_codigo: str


class ImportResultado(BaseModel):
    total: int
    produto_nome: str
    sku: str
    registros: list[RegistroImportado]


__all__ = [
    "GuruPedidoRow",
    "ImportResultado",
    "ImportacaoParams",
    "RegistroImportado",
]
