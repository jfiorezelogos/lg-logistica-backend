from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.schemas.catalogo import (
    AssinaturaPatch,
    ComboPatch,
    IdIntIn,  # se você já usa
    IdStrIn,
    ItemCreate,
    ProdutoPatch,
    SKUsPayload,
)
from app.services.loader_catalogo import (
    carregar_skus,
    salvar_skus,
)

router = APIRouter(prefix="/catalogo", tags=["Catálogo / SKUs"])


# =========================
# Helpers internos
# =========================


def _assert_sku_unico(skus: dict[str, dict[str, Any]], sku: str, nome_atual: str | None = None) -> None:
    iguais = [
        n for n, info in skus.items() if str(info.get("sku") or "") == sku and (nome_atual is None or n != nome_atual)
    ]
    if iguais:
        raise HTTPException(
            status_code=409,
            detail={"erro": "duplicidade_sku", "mensagem": "SKU já existe em outro item.", "nomes": iguais},
        )


def _resolver_por_sku(skus: dict[str, dict[str, Any]], sku: str) -> tuple[str, dict[str, Any]]:
    hits = [(nome, info) for nome, info in skus.items() if str(info.get("sku") or "") == sku]
    if not hits:
        raise HTTPException(status_code=404, detail="Item não encontrado por SKU")
    if len(hits) > 1:
        raise HTTPException(
            status_code=409,
            detail={
                "erro": "duplicidade_sku",
                "mensagem": "Há mais de um item com esse SKU. Ajuste o catálogo para ter SKUs únicos.",
                "nomes_afetados": [n for n, _ in hits],
            },
        )
    return hits[0]


# =========================
# Endpoints básicos
# =========================


@router.get(
    "/",
    summary="Listar SKUs (arquivo completo)",
    description="Retorna o conteúdo do `skus.json` como dict nome→info (formato atual do arquivo).",
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


# ==============
# CREATE (POST)
# ==============


@router.post(
    "/{sku}",
    summary="Criar item por SKU (produto | combo | assinatura)",
    description=(
        "Cria um item identificado por `sku`. Se já existir um item com esse SKU, retorna 409.\n"
        "- `tipo='produto'`: ignora `composto_de`.\n"
        "- `tipo='combo'`: exige `composto_de` (lista de SKUs).\n"
        "- `tipo='assinatura'`: exige `recorrencia` e `periodicidade` ('mensal' | 'bimestral')."
    ),
)
def criar_item_por_sku(sku: str, body: ItemCreate) -> dict[str, dict[str, Any]]:
    sku = (sku or "").strip()
    if not sku:
        raise HTTPException(status_code=422, detail="SKU obrigatório")
    try:
        skus = carregar_skus()

        # 409 se já houver esse SKU
        existentes = [n for n, i in skus.items() if str(i.get("sku") or "") == sku]
        if existentes:
            raise HTTPException(
                status_code=409,
                detail={"erro": "sku_existente", "mensagem": "Já existe item com esse SKU.", "nomes": existentes},
            )

        info: dict[str, Any] = {
            "sku": sku,
            "peso": body.peso,
            "guru_ids": body.guru_ids,
            "shopify_ids": body.shopify_ids,
            "indisponivel": body.indisponivel,
        }

        if body.tipo == "produto":
            info.update({"tipo": "produto", "composto_de": []})
        elif body.tipo == "combo":
            info.update({"tipo": "combo", "composto_de": body.composto_de})
        else:  # assinatura
            info.update(
                {
                    "tipo": "assinatura",
                    "recorrencia": body.recorrencia,
                    "periodicidade": body.periodicidade,
                    "composto_de": [],  # por consistência no arquivo
                }
            )

        if body.preco_fallback is not None:
            info["preco_fallback"] = body.preco_fallback

        nome_key = (body.nome or "").strip()
        if not nome_key:
            raise HTTPException(status_code=422, detail="Nome obrigatório")

        _assert_sku_unico(skus, sku, nome_atual=None)
        skus[nome_key] = info
        salvar_skus(skus)
        return skus
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao criar item por SKU: {e}")


# =========================
# Remoção (APENAS por SKU)
# =========================


@router.delete(
    "/{sku}",
    summary="Remover item por SKU",
    description=(
        "Remove um item (produto/combo) procurando pelo campo `sku`. "
        "Se houver mais de um item com o mesmo SKU, retorna 409 (duplicidade). "
        "Assinaturas normalmente têm `sku=''` e não são elegíveis por este endpoint."
    ),
    response_model=dict[str, dict[str, Any]],
)
def remover_item_por_sku(sku: str) -> dict[str, dict[str, Any]]:
    try:
        sku = (sku or "").strip()
        if not sku:
            raise HTTPException(status_code=422, detail="SKU inválido")

        skus = carregar_skus()
        nome, _ = _resolver_por_sku(skus, sku)

        del skus[nome]
        salvar_skus(skus)
        return skus
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao remover por SKU: {e}")


# =========================
# Disponibilidade (APENAS por SKU)
# =========================


class IndisponibilidadeIn(BaseModel):
    indisponivel: bool


@router.patch(
    "/{sku}/indisponivel",
    summary="Marcar indisponibilidade por SKU",
    description="Procura o item pelo campo `sku` e define `indisponivel`.",
)
def set_indisponivel_por_sku(sku: str, body: IndisponibilidadeIn) -> dict[str, Any]:
    try:
        sku = (sku or "").strip()
        if not sku:
            raise HTTPException(status_code=422, detail="SKU inválido")

        skus = carregar_skus()
        nome, info = _resolver_por_sku(skus, sku)

        info["indisponivel"] = bool(body.indisponivel)
        salvar_skus(skus)
        return {"nome": nome, **info}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao marcar indisponível por SKU: {e}")


# =========================
# PATCH parcial (merge) por SKU
# =========================


@router.patch(
    "/{sku}",
    summary="Patch parcial por SKU (merge)",
    description=(
        "Atualiza parcialmente os campos do item identificado pelo `sku`. "
        "produto → ProdutoPatch | combo → ComboPatch | assinatura → AssinaturaPatch."
    ),
)
def patch_por_sku(sku: str, body: dict[str, Any]) -> dict[str, Any]:
    try:
        sku = (sku or "").strip()
        if not sku:
            raise HTTPException(status_code=422, detail="SKU inválido")

        skus = carregar_skus()
        nome, info = _resolver_por_sku(skus, sku)

        tipo = str(info.get("tipo") or "produto")

        patch: BaseModel
        if tipo == "combo":
            patch = ComboPatch.model_validate(body)
        elif tipo == "assinatura":
            patch = AssinaturaPatch.model_validate(body)
        else:
            patch = ProdutoPatch.model_validate(body)

        data = patch.model_dump(exclude_unset=True)
        for k, v in data.items():
            info[k] = v

        salvar_skus(skus)
        return {"nome": nome, **info}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha no patch por SKU: {e}")


# =========================
# Endpoints atômicos para IDs (Guru/Shopify)
# =========================


@router.post(
    "/{sku}/gid",
    summary="Adicionar um Guru ID ao item (por SKU)",
)
def add_guru_id(sku: str, body: IdStrIn) -> dict[str, Any]:
    skus = carregar_skus()
    nome, info = _resolver_por_sku(skus, (sku or "").strip())
    ids = list(info.get("guru_ids") or [])
    if body.id not in ids:
        ids.append(body.id)
        info["guru_ids"] = ids
        salvar_skus(skus)
    return {"nome": nome, **info}


@router.delete(
    "/{sku}/{gid}",
    summary="Remover um Guru ID do item (por SKU)",
)
def remove_guru_id(sku: str, gid: str) -> dict[str, Any]:
    skus = carregar_skus()
    nome, info = _resolver_por_sku(skus, (sku or "").strip())
    info["guru_ids"] = [x for x in (info.get("guru_ids") or []) if x != gid]
    salvar_skus(skus)
    return {"nome": nome, **info}


@router.post(
    "/{sku}/sid",
    summary="Adicionar um Shopify ID ao item (por SKU)",
)
def add_shopify_id(sku: str, body: IdIntIn) -> dict[str, Any]:
    skus = carregar_skus()
    nome, info = _resolver_por_sku(skus, (sku or "").strip())
    ids = list(info.get("shopify_ids") or [])
    if body.id not in ids:
        ids.append(body.id)
        info["shopify_ids"] = ids
        salvar_skus(skus)
    return {"nome": nome, **info}


@router.delete(
    "/{sku}/{sid}",
    summary="Remover um Shopify ID do item (por SKU)",
)
def remove_shopify_id(sku: str, sid: int) -> dict[str, Any]:
    skus = carregar_skus()
    nome, info = _resolver_por_sku(skus, (sku or "").strip())
    info["shopify_ids"] = [x for x in (info.get("shopify_ids") or []) if int(x) == int(x) and x != sid]
    salvar_skus(skus)
    return {"nome": nome, **info}


# =========================
# Consulta (APENAS por SKU)
# =========================


@router.get(
    "/{sku}",
    summary="Obter item por SKU",
    description="Retorna o item (produto/combo) cujo `sku` coincida (assinaturas normalmente não têm SKU).",
)
def obter_por_sku(sku: str) -> dict[str, Any]:
    try:
        sku = (sku or "").strip()
        if not sku:
            raise HTTPException(status_code=422, detail="SKU inválido")

        skus = carregar_skus()
        nome, info = _resolver_por_sku(skus, sku)
        return {"nome": nome, **info}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao obter item por SKU: {e}")
