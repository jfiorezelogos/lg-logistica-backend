from __future__ import annotations

from pathlib import Path
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from app.schemas.guru_vendas_assinaturas import ColetaOut, PersistenciaPlanilha
from app.services.guru_vendas_assinaturas import (
    garantir_dedup_ids_assinaturas,  # ✅ validador de dedup_id
    montar_payload_busca_assinaturas,
)
from app.services.guru_worker_coleta import executar_worker_guru
from app.services.loader_main import carregar_cfg, carregar_skus
from app.services.loader_regras_assinaturas import (
    montar_mapas_cupons,
    montar_ofertas_embutidas,
    normalizar_rules,
)

# persistência em planilha (JSON) com dedupe/merge
from app.storage.planilhas import append_coleta

router = APIRouter(prefix="/guru/pedidos", tags=["Coletas"])

BASE_DIR = Path(__file__).resolve().parents[2]
SKUS_PATH = BASE_DIR / "skus.json"
CFG_PATH = BASE_DIR / "config_ofertas.json"


@router.get(
    "/assinaturas",
    response_model=ColetaOut,
    summary="Coletar assinaturas",
    description=(
        "Executa a **coleta completa** e retorna todas as linhas em `linhas`, junto com `contagem`.\n\n"
        "⚠️ **Obrigatório** informar `planilha_id`: as linhas serão **acrescentadas** na planilha (JSON) com deduplicação.\n"
        "Regras de dedupe (dedup_id obrigatório):\n"
        "- **Linha principal**: `transaction_id`\n"
        "- **Derivadas** (combo, brinde por cupom, embutido por oferta): `transaction_id+SKU`\n"
        "A planilha deve ser criada antes em `/planilhas/criar`."
    ),
    response_description="Retorna todas as linhas coletadas (`linhas`) e o resumo (`contagem`).",
)
def coletar_assinaturas(
    ano: int,
    mes: int,
    box_sku: str,
    modo_periodo: int = Query(..., ge=0, le=1, description="1 = PERÍODO, 0 = TODAS", example=1),
    periodicidade: Literal["mensal", "bimestral"] = Query(
        ..., description="Periodicidade da assinatura", example="mensal"
    ),
    planilha_id: str = Query(
        ...,
        description=(
            "ID da planilha (JSON) já criada em `/planilhas/criar` para persistir as linhas com dedupe "
            "(`transaction_id` na principal; `transaction_id+SKU` nas derivadas)."
        ),
        example="pln_20251002_154522_ab12cd",
    ),
) -> ColetaOut:
    try:
        skus_info = carregar_skus()
        cfg = carregar_cfg()

        # resolve nome do box pelo SKU
        box_entry = next((nome for nome, info in skus_info.items() if info.get("sku") == box_sku), None)
        if not box_entry:
            raise HTTPException(status_code=400, detail=f"SKU '{box_sku}' não encontrado no skus.json")

        modo_str = "PERÍODO" if modo_periodo == 1 else "TODAS"

        ofertas_embutidas = montar_ofertas_embutidas(cfg)
        cupons_cdf, cupons_bi_mens = montar_mapas_cupons(cfg)

        dados = montar_payload_busca_assinaturas(
            ano=ano,
            mes=mes,
            modo_periodo=modo_str,
            box_nome=box_entry,
            periodicidade=periodicidade,
            skus_info=skus_info,
        )
        dados["rules"] = normalizar_rules(cfg)
        dados["ofertas_embutidas"] = ofertas_embutidas
        dados["cupons_personalizados_cdf"] = cupons_cdf
        dados["cupons_personalizados_bi_mens"] = cupons_bi_mens
        dados["cupons_personalizados_anual"] = cupons_cdf
        dados["cupons_personalizados_bimestral"] = cupons_bi_mens

        # 1) coleta
        linhas, contagem = executar_worker_guru(dados, skus_info=skus_info)

        # 2) valida dedup_id (não-destrutivo): exige transaction_id e preenche dedup_id
        #    apenas quando estiver ausente (linha principal); não mexe nas derivadas já setadas (transaction_id:SKU)
        try:
            garantir_dedup_ids_assinaturas(linhas)
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

        # 3) persistir na planilha (dedupe por dedup_id + merge de campos em linhas existentes)
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

        return ColetaOut(linhas=linhas, contagem=contagem, persistencia=persistencia)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Falha na coleta: {e!s}")
