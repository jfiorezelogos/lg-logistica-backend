from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from datetime import date
from typing import Optional, Sequence
from services.mapeamento import load_skus_info
from app.services.coleta_vendas_produtos import iniciar_coleta_vendas_produtos

router = APIRouter()

class ColetaProdutosIn(BaseModel):
    data_ini: date
    data_fim: date
    nome_produto: Optional[str] = None
    transportadoras_permitidas: Sequence[str] = ()

class ColetaProdutosOut(BaseModel):
    modo: str
    inicio: str
    fim: str
    produtos_ids: list[str]
    transportadoras_permitidas: list[str]

@router.post("/coletas/vendas-produtos", response_model=ColetaProdutosOut)
def iniciar(req: ColetaProdutosIn):
    try:
        # TODO: skus_info deve vir do seu domínio/repositorio
        skus_info = load_skus_info()  

        payload = iniciar_coleta_vendas_produtos(
            data_ini=req.data_ini,
            data_fim=req.data_fim,
            nome_produto=req.nome_produto,
            skus_info=skus_info,
            transportadoras_permitidas=req.transportadoras_permitidas,
        )
        return payload
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
