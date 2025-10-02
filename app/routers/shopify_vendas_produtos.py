# /routers/shopify_vendas_produtos.py

from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.shopify_vendas_produtos import coletar_vendas_shopify

router = APIRouter(prefix="/shopify", tags=["Coletas"])


@router.get("/pedidos")
def get_shopify_pedidos(
    status: str = Query("any", pattern="^(any|unfulfilled)$"),
    data_inicio: str = Query(..., description="YYYY-MM-DD"),  # data no padrão ISO
    gpt: bool = Query(False, description="Ativa ajuste de endereço com IA na normalização"),
) -> dict[str, Any]:
    # 1) Converter data_inicio para dd/MM/yyyy (padrão interno do service)
    try:
        _dt = datetime.strptime(data_inicio, "%Y-%m-%d")
        data_inicio_ddmmyyyy = _dt.strftime("%d/%m/%Y")
    except ValueError:
        raise HTTPException(status_code=400, detail='data_inicio inválida: use "YYYY-MM-DD"')

    # 2) Coleta completa -> linhas no layout da planilha (padronizar_planilha_bling)
    try:
        linhas, stats = coletar_vendas_shopify(
            data_inicio=data_inicio_ddmmyyyy,
            fulfillment_status=status,
            sku_produtos=None,
            enrich_cpfs=True,  # sempre injeta CPF quando disponível
            enrich_bairros=True,  # sempre busca bairro via CEP (lote/cache)
            enrich_enderecos=True,  # sempre normaliza endereço
        )
        # Se quiser IA às vezes, conecte seu provider dentro de enriquecer_enderecos_nas_linhas
        # quando gpt=True (ex.: via variável global ou parâmetro adicional no pipeline).
        # Aqui apenas propagamos a flag para facilitar debugging futuro:
        stats.setdefault("flags", {})["gpt"] = gpt
    except Exception:
        raise HTTPException(status_code=502, detail="Erro ao coletar/normalizar pedidos da Shopify")

    # 3) Retorno JSON no formato “planilha” (mesmas colunas do Bling)
    # Ex.: linhas: List[Dict[str, str]] com chaves como:
    # "Número pedido", "Nome Comprador", "CPF/CNPJ Comprador", "Endereço Entrega", "Número Entrega", ...
    return {
        "linhas": linhas,  # dados já prontos no shape da planilha
        "stats": stats,  # contagens auxiliares (status/produto etc.)
        "filtros": {
            "status": status,
            "data_inicio": data_inicio,  # ecoa a data ISO solicitada
        },
    }
