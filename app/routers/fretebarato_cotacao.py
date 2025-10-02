from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.schemas.fretebarato_cotacao import CotarFretesAutoRequest, CotarFretesResponse
from app.services.fretebarato_cotacao import cotar_fretes_auto, get_planilha_atual  # type: ignore[attr-defined]

router = APIRouter(prefix="/fretebarato", tags=["Cotações"])


@router.post(
    "/cotar",
    response_model=CotarFretesResponse,
    summary="Cotar frete (Frete Barato) montando lotes automaticamente por email+cep a partir do snapshot coletado",
)
def cotar_fretes_endpoint(req: CotarFretesAutoRequest) -> CotarFretesResponse:
    linhas, _ = get_planilha_atual()
    if not linhas:
        # Nenhuma coleta disponível ainda
        raise HTTPException(status_code=412, detail="Nenhum snapshot de pedidos coletados está disponível")
    try:
        return cotar_fretes_auto(req)
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=502, detail="Erro ao cotar fretes")
