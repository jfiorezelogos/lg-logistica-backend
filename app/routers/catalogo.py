from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.skus_service import (
    carregar_skus, salvar_skus, gerar_chave_assinatura,
)
from app.schemas.skus_service import ProdutoIn, AssinaturaIn, ComboIn, SKUsPayload

router = APIRouter(prefix="/catalogo/skus", tags=["Catálogo / SKUs"])


# ======== Endpoints básicos ========

@router.get(
    "/",
    summary="Listar SKUs (arquivo completo)",
    description="Retorna o conteúdo do `skus.json` como dict nome→info."
)
def listar_skus() -> dict[str, dict[str, Any]]:
    try:
        return carregar_skus()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar SKUs: {e}")


@router.put(
    "/",
    summary="Substituir skus.json (completo)",
    description="Sobrescreve o arquivo `skus.json` com o payload enviado.",
    response_model=dict[str, dict[str, Any]],
)
def substituir_skus(body: SKUsPayload) -> dict[str, dict[str, Any]]:
    try:
        salvar_skus(body.skus)
        return carregar_skus()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao salvar SKUs: {e}")


# ======== Upserts ========

@router.post(
    "/produto",
    summary="Upsert de produto simples",
    description="Cria ou atualiza um **produto** (tipo='produto'). A chave é o `nome`."
)
def upsert_produto(prod: ProdutoIn) -> dict[str, dict[str, Any]]:
    try:
        skus = carregar_skus()
        skus[prod.nome] = {
            "sku": prod.sku,
            "peso": prod.peso,
            "guru_ids": prod.guru_ids,
            "shopify_ids": prod.shopify_ids,
            "tipo": "produto",
            "composto_de": [],
            "indisponivel": prod.indisponivel,
        }
        if prod.preco_fallback is not None:
            skus[prod.nome]["preco_fallback"] = prod.preco_fallback
        salvar_skus(skus)
        return skus
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha no upsert do produto: {e}")


@router.post(
    "/assinatura",
    summary="Upsert de assinatura",
    description="Cria ou atualiza uma **assinatura** (tipo='assinatura'). A chave é `nome_base - periodicidade`."
)
def upsert_assinatura(assin: AssinaturaIn) -> dict[str, dict[str, Any]]:
    try:
        skus = carregar_skus()
        key = gerar_chave_assinatura(assin.nome_base, assin.periodicidade)
        skus[key] = {
            "tipo": "assinatura",
            "recorrencia": (assin.recorrencia or "").strip(),
            "periodicidade": assin.periodicidade,
            "guru_ids": assin.guru_ids,
            "shopify_ids": [],
            "composto_de": [],
            "sku": "",
            "peso": 0.0,
            "indisponivel": assin.indisponivel,
        }
        if assin.preco_fallback is not None:
            skus[key]["preco_fallback"] = assin.preco_fallback
        salvar_skus(skus)
        return skus
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha no upsert da assinatura: {e}")


@router.post(
    "/combo",
    summary="Upsert de combo",
    description="Cria ou atualiza um **combo** (tipo='combo'). A chave é o `nome`."
)
def upsert_combo(combo: ComboIn) -> dict[str, dict[str, Any]]:
    try:
        skus = carregar_skus()
        skus[combo.nome] = {
            "sku": combo.sku,
            "peso": 0.0,
            "tipo": "combo",
            "composto_de": combo.composto_de,
            "guru_ids": combo.guru_ids,
            "shopify_ids": combo.shopify_ids,
            "indisponivel": combo.indisponivel,
        }
        if combo.preco_fallback is not None:
            skus[combo.nome]["preco_fallback"] = combo.preco_fallback
        salvar_skus(skus)
        return skus
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha no upsert do combo: {e}")


# ======== Remoção ========

class DeleteBody(BaseModel):
    nome: str


@router.delete(
    "/",
    summary="Remover item por nome",
    description="Remove um item (produto/assinatura/combo) pela **chave do dicionário** (nome)."
)
def remover_item(body: DeleteBody) -> dict[str, dict[str, Any]]:
    try:
        skus = carregar_skus()
        nome = (body.nome or "").strip()
        if not nome or nome not in skus:
            raise HTTPException(status_code=404, detail="Item não encontrado")
        del skus[nome]
        salvar_skus(skus)
        return skus
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao remover item: {e}")
