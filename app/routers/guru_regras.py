# app/routers/guru_regras.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

# modelos pydantic centralizados
from app.schemas.guru_regras import ConfigOfertas, Regra

# service de regras (backend puro)
from app.services import guru_regras as regras

router = APIRouter(prefix="/guru/regras", tags=["Regras de Cupons/Ofertas"])

BASE_DIR = Path(__file__).resolve().parents[2]
CFG_PATH = BASE_DIR / "config_ofertas.json"

RegraList = list[dict[str, Any]]  # resposta “bruta” (shape do arquivo)


# ======================= Helpers (id → idx) =======================


def _load_rules() -> list[dict[str, Any]]:
    return regras.carregar_regras(CFG_PATH)


def _save_rules(rules: list[dict[str, Any]]) -> None:
    regras.salvar_regras(CFG_PATH, rules)


def _find_idx_by_id(rules: list[dict[str, Any]], rid: str) -> int:
    for i, r in enumerate(rules):
        if str(r.get("id") or "") == rid:
            return i
    raise HTTPException(status_code=404, detail="Regra não encontrada (id inválido)")


# ======================= Endpoints =======================


@router.get(
    "/",
    response_model=RegraList,
    summary="Listar regras",
    description="Retorna todas as regras carregadas do arquivo `config_ofertas.json`.",
)
def listar_regras() -> RegraList:
    try:
        return _load_rules()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar regras: {e}")


@router.put(
    "/",
    response_model=ConfigOfertas,
    summary="Substituir todas as regras",
    description="""
⚠️ Sobrescreve **todas** as regras salvas em `config_ofertas.json`.

- O conteúdo atual será descartado.
- Apenas as regras enviadas no corpo permanecerão.

Use este endpoint para **importação em massa** ou **reset**. Para operações individuais, use `POST /`, `PUT /{id}`, `DELETE /{id}`.
""",
)
def substituir_todas_regras(body: ConfigOfertas) -> ConfigOfertas:
    try:
        _save_rules([r.model_dump() for r in body.rules])
        # recarrega do disco para devolver no mesmo formato do arquivo
        return ConfigOfertas(rules=[Regra.model_validate(r) for r in _load_rules()])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao salvar regras: {e}")


@router.post(
    "/",
    response_model=RegraList,
    summary="Adicionar regra",
    description="Adiciona uma nova regra. Se `id` não for enviado, será gerado automaticamente.",
)
def adicionar_regra(regra_in: Regra) -> RegraList:
    try:
        rules = _load_rules()
        regras.add_regra(rules, regra_in.model_dump())
        _save_rules(rules)
        return rules
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao adicionar regra: {e}")


@router.put(
    "/{regra_id}",
    response_model=RegraList,
    summary="Editar regra",
    description="Edita a regra identificada por `id`. Mantém o `id` original.",
)
def editar_regra(regra_id: str, regra_in: Regra) -> RegraList:
    try:
        rules = _load_rules()
        idx = _find_idx_by_id(rules, regra_id)
        regras.edit_regra(rules, idx, regra_in.model_dump())
        _save_rules(rules)
        return rules
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao editar regra: {e}")


@router.delete(
    "/{regra_id}",
    response_model=RegraList,
    summary="Remover regra",
    description="Remove a regra identificada por `id`.",
)
def remover_regra(regra_id: str) -> RegraList:
    try:
        rules = _load_rules()
        idx = _find_idx_by_id(rules, regra_id)
        regras.del_regra(rules, idx)
        _save_rules(rules)
        return rules
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao remover regra: {e}")


@router.get(
    "/contexto",
    response_model=dict[str, Any],
    summary="Contexto inicial",
    description="Retorna `{ rules, produtos_guru, config_path }`, equivalente ao estado inicial que a UI carregava.",
)
def contexto_gerenciador() -> dict[str, Any]:
    try:
        return regras.iniciar_gerenciador_regras_backend(config_path=CFG_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao carregar contexto: {e}")


@router.get(
    "/produtos",
    response_model=list[dict[str, Any]],
    summary="Listar produtos do Guru",
    description="Consulta a API do Guru e retorna a lista completa de produtos (paginada automaticamente).",
)
def listar_produtos_guru(limit: int = Query(100, ge=1, le=500)) -> list[dict[str, Any]]:
    try:
        return regras.coletar_produtos_guru(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao listar produtos do Guru: {e}")
