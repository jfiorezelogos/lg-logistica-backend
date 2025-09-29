# routers/guru_produtos.py
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.guru_produtos import coletar_produtos_guru

router = APIRouter(prefix="/guru", tags=["Coleta"])


class GuruProdutosPageResponse(BaseModel):
    count: int = Field(..., description="Itens retornados nesta página")
    next_cursor: str | None = Field(None, description="Cursor para próxima página (se houver)")
    data: list[dict[str, Any]]


@router.get(
    "/produtos",
    response_model=GuruProdutosPageResponse,
    summary="Listar produtos do Guru",
)
def listar_produtos_guru(
    limit: int = Query(100, ge=1, le=100, description="Qtde por página (máx 100)"),
    cursor: str | None = Query(None, description="Cursor da próxima página fornecido pela página anterior"),
) -> GuruProdutosPageResponse:
    try:
        payload = coletar_produtos_guru(limit=limit, cursor=cursor)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Falha ao buscar produtos do Guru: {e}")

    data = payload.get("data") or []
    next_cursor = payload.get("next_cursor")
    return GuruProdutosPageResponse(count=len(data), next_cursor=next_cursor, data=data)
