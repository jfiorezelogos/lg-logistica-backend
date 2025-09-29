from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile

from app.schemas.guru_importar_planilha import (  # <- confira o nome do módulo/schema
    ImportacaoParams,  # modelo de entrada (form) com sku
    ImportResultado,  # modelo de saída
    RegistroImportado,  # item da saída
)
from app.services.guru_importar_planilha import importar as importar_service

router = APIRouter(prefix="/importar", tags=["Importação"])


@router.post(
    "/guru-planilha",
    response_model=ImportResultado,
    summary="Importar planilha do Guru",
    description=(
        "Recebe um arquivo CSV/XLSX exportado do Guru via multipart/form-data. "
        "Campos do formulário: `sku` (SKU conforme skus.json) e `file` (planilha)."
    ),
)
async def importar_guru_planilha(
    file: UploadFile = File(..., description="Planilha do Guru (.csv ou .xlsx)"),
    params: ImportacaoParams = Depends(ImportacaoParams.as_form),
) -> ImportResultado:
    """
    - `file`: arquivo CSV/XLSX do Guru
    - `params.sku`: SKU do produto (conforme `skus.json`)
    """
    try:
        payload = importar_service(await file.read(), file.filename or "", params.sku)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    registros = [RegistroImportado.model_validate(r) for r in payload["registros"]]
    return ImportResultado(
        total=payload["total"],
        produto_nome=payload["produto_nome"],
        sku=payload["sku"],
        registros=registros,
    )
