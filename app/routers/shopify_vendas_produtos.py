# /routers/shopify_vendas_produtos.py

from fastapi import APIRouter, HTTPException, Query
from datetime import datetime

from app.schemas.shopify_vendas_produtos import PedidosResponse, ShopifyPedidosQuery
from app.services.shopify_ajuste_endereco import buscar_cep_com_timeout, normalizar_endereco_unico
from app.services.shopify_vendas_produtos import listar_pedidos_shopify  # use a função existente

router = APIRouter(prefix="/shopify", tags=["shopify"])


@router.get("/pedidos", response_model=PedidosResponse)
def get_shopify_pedidos(
    status: str = Query("any", pattern="^(any|unfulfilled)$"),
    data_inicio: str = Query(..., description="YYYY-MM-DD"),  # recebido no padrão ISO
    cursor: str | None = Query(None),
    limit: int = Query(50, ge=1, le=50),
    normalize_endereco: bool = Query(False),
):
    # 0) Converter data_inicio ISO -> dd/MM/yyyy (padrão do service)
    try:
        _dt = datetime.strptime(data_inicio, "%Y-%m-%d")
        data_inicio_ddmmyyyy = _dt.strftime("%d/%m/%Y")
    except ValueError:
        raise HTTPException(status_code=400, detail='data_inicio inválida: use "YYYY-MM-DD"')

    # 1) Validação (opcional)
    _ = ShopifyPedidosQuery(
        status=status,
        data_inicio=data_inicio,
        cursor=cursor,
        limit=limit,
        normalize_endereco=normalize_endereco,
    )

    try:
        # 2) Chamada ao service (usa GraphQL e paginação existentes)
        pedidos, next_cursor = listar_pedidos_shopify(
            data_inicio=data_inicio_ddmmyyyy,  # <- convertido
            status=status,
            cursor=cursor,
            limit=limit,
        )

    except Exception as e:
        if getattr(e, "status_code", None) == 429:
            retry_after = getattr(e, "retry_after", 2)
            raise HTTPException(
                status_code=429,
                detail="Rate limited by Shopify",
                headers={"Retry-After": str(retry_after)},
            )
        raise HTTPException(status_code=502, detail="Erro ao consultar a Shopify")

    # 3) (Opcional) Enriquecimento de endereço
    if normalize_endereco and pedidos:
        for p in pedidos:
            addr = (p.get("shippingAddress") or {}) if isinstance(p, dict) else getattr(p, "shippingAddress", None)
            if not addr:
                continue

            order_id = str(p.get("id") or p.get("name") or "")
            address1 = str(addr.get("address1") or "")
            address2 = str(addr.get("address2") or "")
            cep = str(addr.get("zip") or "")

            try:
                norm = normalizar_endereco_unico(
                    order_id=order_id,
                    address1=address1,
                    address2=address2,
                    cep=cep or None,
                    ai_provider=None,
                )
                if isinstance(addr, dict):
                    addr["address1"] = norm.get("endereco_base") or address1
                    addr["address2"] = norm.get("complemento") or address2
                    addr.setdefault("numero", norm.get("numero"))
                    addr.setdefault("precisa_contato", norm.get("precisa_contato"))   # <- remova se não tiver no schema
                    if norm.get("logradouro_oficial"):
                        addr.setdefault("logradouro_oficial", norm["logradouro_oficial"])
                    if norm.get("bairro_oficial"):
                        addr.setdefault("bairro_oficial", norm["bairro_oficial"])

                if cep:
                    dados_cep = buscar_cep_com_timeout(cep, timeout=3)
                    if dados_cep and isinstance(addr, dict):
                        addr.setdefault("bairro_oficial", dados_cep.get("district"))
                        addr.setdefault("logradouro_oficial", dados_cep.get("street"))
            except Exception:
                pass

    return PedidosResponse(items=pedidos, next_cursor=next_cursor)