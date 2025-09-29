# app/services/coleta_guru.py
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from app.services.bling_planilha import montar_planilha_vendas_guru


def executar_worker_guru(
    dados: Mapping[str, Any],
    *,
    skus_info: Any,
    logger: Any | None = None,
) -> tuple[list[Any], dict[str, Any]]:
    """
    Versão backend pura (sem UI, sem QThread, sem estado/cancelador):
    executa o fluxo que antes ficava em WorkerThreadGuru.run().

    Parâmetros:
      - dados: payload com 'modo' ('assinaturas' | 'produtos') e parâmetros da coleta
      - skus_info: mapeamento SKUs

    Retorna:
      (novas_linhas, contagem)
    """
    if logger is None:

        class _NullLogger:
            def info(self, *a, **k):
                pass

            def warning(self, *a, **k):
                pass

            def exception(self, *a, **k):
                pass

        logger = _NullLogger()

    novas_linhas: list[Any] = []
    contagem: dict[str, Any] = {}

    try:
        modo = (cast(str, dados.get("modo") or "assinaturas")).strip().lower()
        logger.info("worker_started", extra={"modo": modo})

        # --- Busca transações (backend puro) ---
        if modo == "assinaturas":
            # ✅ passa skus_info exigido pela função
            from app.services.guru_vendas_assinaturas import gerenciar_coleta_vendas_assinaturas

            transacoes, _, dados_final_map = gerenciar_coleta_vendas_assinaturas(
                cast(dict[str, Any], dict(dados)),
                skus_info=skus_info,
            )
        elif modo == "produtos":
            from app.services.guru_vendas_produtos import coletar_vendas_produtos

            transacoes, _, dados_final_map = coletar_vendas_produtos(  # ajuste se sua assinatura exigir skus_info
                cast(dict[str, Any], dict(dados))
            )
        else:
            raise ValueError(f"Modo de busca desconhecido: {modo}")

        if not isinstance(dados_final_map, Mapping):
            raise ValueError("Dados inválidos retornados da busca.")
        dados_final: dict[str, Any] = dict(dados_final_map)

        if not isinstance(transacoes, list) or not isinstance(dados_final, dict):
            raise ValueError("Dados inválidos retornados da busca.")

        logger.info("worker_received_transactions", extra={"qtd": len(transacoes), "modo": modo})

        # --- Montagem da planilha/linhas (backend puro) ---
        novas_linhas, contagem_map = montar_planilha_vendas_guru(
            transacoes=transacoes,
            dados=dados_final,
            skus_info=skus_info,
        )

        if not isinstance(contagem_map, Mapping):
            raise ValueError("Retorno inválido de montar_planilha_vendas_guru (esperado Mapping).")
        contagem = dict(contagem_map)

        logger.info("worker_success", extra={"linhas_adicionadas": len(novas_linhas)})

    except Exception as e:
        logger.exception("worker_error", extra={"err": str(e)})
        raise
    finally:
        logger.info("worker_finished")

    return novas_linhas, contagem
