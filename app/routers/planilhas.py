from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas.planilhas import CreatePlanilhaRequest, CreatePlanilhaResponse
from app.storage.planilhas import create_planilha, load_planilha

router = APIRouter(prefix="/planilhas", tags=["Pré-requisitos"])


@router.post("/criar", response_model=CreatePlanilhaResponse, summary="Criar planilha (ID escolhido pelo cliente)")
def criar_planilha(payload: CreatePlanilhaRequest) -> CreatePlanilhaResponse:
    try:
        planilha_id = create_planilha(payload.planilha_id, meta=payload.meta)
    except FileExistsError:
        raise HTTPException(status_code=409, detail="Já existe uma planilha com esse ID")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Falha ao criar planilha: {e}")

    meta = load_planilha(planilha_id)

    return CreatePlanilhaResponse(
        planilha_id=planilha_id,
        created_at=str(meta.get("created_at", "")),  # horário local (America/Sao_Paulo)
    )
