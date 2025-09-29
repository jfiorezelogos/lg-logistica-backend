from __future__ import annotations

import io
from typing import Any

import pandas as pd

from app.services.loader_produtos_info import get_produto_info, is_indisponivel, load_skus
from app.services.loader_regras_assinaturas import (
    TABELA_VALORES,
    divisor_para,
    eh_assinatura,
    inferir_periodicidade,
    inferir_tipo,
)
from app.utils.utils_helpers import limpar, parse_money


def importar(file_bytes: bytes, filename: str, sku: str) -> dict[str, Any]:
    info = get_produto_info(sku)
    if not info:
        raise ValueError(f"SKU '{sku}' não encontrado no skus.json")
    produto_nome = next((nome for nome, i in load_skus().items() if i.get("sku") == sku), sku)
    buf = io.BytesIO(file_bytes)
    fname = (filename or "").lower()

    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(buf, sep=";", encoding="utf-8", quotechar='"', dtype=str)
        elif fname.endswith(".xlsx"):
            df = pd.read_excel(buf)  # requer openpyxl
        else:
            raise ValueError("Extensão não suportada (use .csv ou .xlsx)")
    except Exception as e:
        raise ValueError(f"Erro ao carregar planilha: {e}") from e

    registros: list[dict[str, Any]] = []

    for _, linha in df.iterrows():
        if pd.isna(linha.get("email contato")) and pd.isna(linha.get("nome contato")):
            continue

        valor_venda = parse_money(linha.get("valor venda", ""))
        nome_prod = str(linha.get("nome produto", ""))
        id_prod = str(linha.get("id produto", ""))
        assinatura_codigo = str(linha.get("assinatura código") or linha.get("assinatura codigo") or "").strip()

        is_assin = eh_assinatura(nome_prod)
        periodicidade = inferir_periodicidade(id_prod) if is_assin else ""
        tipo_ass = inferir_tipo(nome_prod) if is_assin else ""

        usar_fallback = bool(is_assin and assinatura_codigo == "")

        if is_assin:
            if usar_fallback and tipo_ass in {"anuais", "bianuais", "trianuais"}:
                base = float(TABELA_VALORES.get((tipo_ass, periodicidade), valor_venda))
            else:
                base = float(valor_venda)
            div = divisor_para(tipo_ass, periodicidade)
            valor_unitario = round(base / max(div, 1), 2)
            valor_total_item = valor_unitario
        else:
            valor_unitario = valor_venda
            valor_total_item = valor_venda

        total_pedido = valor_venda

        cpf = limpar(linha.get("doc contato")).zfill(11)
        cep = limpar(linha.get("cep contato")).zfill(8)[:8]
        telefone = limpar(linha.get("telefone contato"))

        data_pedido_raw = linha.get("data pedido", "")
        try:
            data_pedido = pd.to_datetime(data_pedido_raw, dayfirst=True).strftime("%d/%m/%Y")
        except Exception:
            data_pedido = pd.Timestamp.today().strftime("%d/%m/%Y")

        registros.append(
            {
                "Número pedido": "",
                "Nome Comprador": limpar(linha.get("nome contato")),
                "Data Pedido": data_pedido,
                "Data": pd.Timestamp.today().strftime("%d/%m/%Y"),
                "CPF/CNPJ Comprador": cpf,
                "Endereço Comprador": limpar(linha.get("logradouro contato")),
                "Bairro Comprador": limpar(linha.get("bairro contato")),
                "Número Comprador": limpar(linha.get("número contato")),
                "Complemento Comprador": limpar(linha.get("complemento contato")),
                "CEP Comprador": cep,
                "Cidade Comprador": limpar(linha.get("cidade contato")),
                "UF Comprador": limpar(linha.get("estado contato")),
                "Telefone Comprador": telefone,
                "Celular Comprador": telefone,
                "E-mail Comprador": limpar(linha.get("email contato")),
                "Produto": produto_nome,
                "SKU": sku,
                "Un": "UN",
                "Quantidade": "1",
                "Valor Unitário": f"{valor_unitario:.2f}".replace(".", ","),
                "Valor Total": f"{valor_total_item:.2f}".replace(".", ","),
                "Total Pedido": f"{total_pedido:.2f}".replace(".", ","),
                "Valor Frete Pedido": "",
                "Valor Desconto Pedido": "",
                "Outras despesas": "",
                "Nome Entrega": limpar(linha.get("nome contato")),
                "Endereço Entrega": limpar(linha.get("logradouro contato")),
                "Número Entrega": limpar(linha.get("número contato")),
                "Complemento Entrega": limpar(linha.get("complemento contato")),
                "Cidade Entrega": limpar(linha.get("cidade contato")),
                "UF Entrega": limpar(linha.get("estado contato")),
                "CEP Entrega": cep,
                "Bairro Entrega": limpar(linha.get("bairro contato")),
                "Transportadora": "",
                "Serviço": "",
                "Tipo Frete": "0 - Frete por conta do Remetente (CIF)",
                "Observações": "",
                "Qtd Parcela": "",
                "Data Prevista": "",
                "Vendedor": "",
                "Forma Pagamento": limpar(linha.get("pagamento")),
                "ID Forma Pagamento": "",
                "transaction_id": limpar(linha.get("id transação")),
                "indisponivel": "S" if is_indisponivel(sku) else "",
                "periodicidade": periodicidade,
                "Plano Assinatura": tipo_ass if is_assin else "",
                "assinatura_codigo": assinatura_codigo,
            }
        )

    return {
        "total": len(registros),
        "produto_nome": produto_nome,
        "sku": sku,
        "registros": registros,
    }
