from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas.guru_vendas_produtos import ColetaOut, PersistenciaPlanilha
from app.services.guru_vendas_produtos import iniciar_coleta_vendas_produtos
from app.services.guru_worker_coleta import executar_worker_guru
from app.services.loader_produtos_info import load_skus_info

# persistência em planilha (JSON) com dedupe por dedup_id (line item)
from app.storage.planilhas import append_coleta

router = APIRouter(prefix="/guru/pedidos", tags=["Coletas"])


@router.get(
    "/produtos",
    response_model=ColetaOut,
    summary="Coletar vendas de produtos",
    description=(
        "Executa a **coleta completa** e retorna **todas as linhas** em `linhas`, com `contagem`.\n\n"
        "⚠️ **Obrigatório** informar `planilha_id`: as linhas serão **acrescentadas** na planilha (JSON) "
        "com **deduplicação por `dedup_id`** — na prática, **`transaction_id`** (principal) e **`transaction_id+SKU`** (itens de combo). "
        "A planilha deve ser criada antes em `/planilhas/criar`."
    ),
    response_description="Retorna todas as linhas (`linhas`) e o resumo (`contagem`).",
)
def buscar_vendas_produtos_guru(
    data_ini: date = Query(..., description="Data inicial (YYYY-MM-DD)"),
    data_fim: date = Query(..., description="Data final (YYYY-MM-DD)"),
    nome_produto: str | None = Query(None, description="Nome do produto; vazio para todos"),
    planilha_id: str = Query(
        ...,
        description=(
            "ID da planilha (JSON) já criada em `/planilhas/criar` para persistir as linhas com dedupe por `dedup_id` "
            "(line item; principal = transaction_id, combo = transaction_id:SKU)."
        ),
        example="pln_20251002_154522_ab12cd",
    ),
) -> ColetaOut:
    try:
        # 1) Carregar SKUs
        skus_info = load_skus_info()

        # 2) Montar payload de coleta
        payload = iniciar_coleta_vendas_produtos(
            data_ini=data_ini,
            data_fim=data_fim,
            nome_produto=nome_produto,
            skus_info=skus_info,
        )

        # 3) Executar coleta
        linhas, contagem = executar_worker_guru(payload, skus_info=skus_info)

        # 4) Garantir dedup_id obrigatório:
        #    - se já vier (ex.: de desmembrar combo), preserva
        #    - se não vier (linha principal), usa transaction_id
        for r in linhas:
            tid = str(r.get("transaction_id") or "").strip()
            if not tid:
                raise HTTPException(status_code=422, detail="Linha sem transaction_id; dedup_id é obrigatório.")
            if not str(r.get("dedup_id") or "").strip():
                # principal
                r["dedup_id"] = tid
            # se a linha for de combo, a função desmembrar_combo_planilha já deve
            # ter setado dedup_id = f"{transaction_id}:{SKU}"

        # 5) Persistir na planilha (dedupe por dedup_id + merge)
        try:
            adicionados, atualizados = append_coleta(planilha_id, linhas)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail=f"planilha_id não encontrada: {planilha_id}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Erro ao persistir na planilha: {e!s}")

        persistencia = PersistenciaPlanilha(
            planilha_id=planilha_id,
            adicionados=adicionados,
            atualizados=atualizados,
        )

        # 6) Retorno completo
        return ColetaOut(linhas=linhas, contagem=contagem, persistencia=persistencia)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta de vendas de produtos: {e!s}")
