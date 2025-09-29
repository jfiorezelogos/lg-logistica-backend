import calendar
import datetime as dt
import random
import time
from collections.abc import Mapping
from typing import Any, cast

import requests
from coleta_vendas_assinaturas import gerenciar_coleta_vendas_assinaturas
from coleta_vendas_produtos import coletar_vendas_produtos
from planilha_bling import montar_planilha_vendas_guru
from requests import Response, Session

from utils.datetime_helpers import _as_dt
from common.settings import settings

UTC = dt.UTC

BASE_URL_GURU = "https://digitalmanager.guru/api/v2"
HEADERS_GURU = {
    "Authorization": f"Bearer {settings.API_KEY_GURU}",
    "Content-Type": "application/json",
}

LIMITE_INFERIOR = dt.datetime(2024, 10, 1, tzinfo=UTC)


class TransientGuruError(Exception):
    """Erro transitório ao buscar a PRIMEIRA página; deve acionar retry externo."""


class TransientPageError(Exception):
    def __init__(self, last_exc: Exception | None):
        super().__init__(str(last_exc) if last_exc else "Falha ao buscar página")
        self.last_exc = last_exc


def dividir_periodos_coleta_api_guru(
    data_inicio: str | dt.date | dt.datetime,
    data_fim: str | dt.date | dt.datetime,
) -> list[tuple[str, str]]:
    """
    Divide o intervalo em blocos com fins em abr/ago/dez.
    Retorna (YYYY-MM-DD, YYYY-MM-DD). Usa dt.datetime aware (UTC).
    """
    ini = _as_dt(data_inicio)
    if not ini.tzinfo:
        ini = ini.replace(tzinfo=UTC)
    end = _as_dt(data_fim)
    if not end.tzinfo:
        end = end.replace(tzinfo=UTC)

    blocos: list[tuple[str, str]] = []
    atual = ini

    while atual <= end:
        ano = atual.year
        mes = atual.month

        # Blocos: jan-abr, mai-ago, set-dez
        fim_mes = 4 if mes <= 4 else (8 if mes <= 8 else 12)

        ultimo_dia = calendar.monthrange(ano, fim_mes)[1]
        fim_bloco = dt.datetime(ano, fim_mes, ultimo_dia, 23, 59, 59, tzinfo=UTC)

        fim_bloco = min(fim_bloco, end)

        blocos.append((atual.date().isoformat(), fim_bloco.date().isoformat()))

        # próximo bloco
        proximo_mes = fim_mes + 1
        proximo_ano = ano
        if proximo_mes > 12:
            proximo_mes = 1
            proximo_ano += 1
        atual = dt.datetime(proximo_ano, proximo_mes, 1, tzinfo=UTC)

    return blocos


def _fetch_page_with_retry(
    session: Session,
    *,
    base_url: str,
    headers: Mapping[str, Any],
    params: dict[str, Any],
    timeout: tuple[float, float],
    max_page_retries: int,
    product_id: str,
) -> Mapping[str, Any]:
    last_exc: Exception | None = None
    for tentativa in range(max_page_retries + 1):
        try:
            r: Response = session.get(f"{base_url}/transactions", headers=headers, params=params, timeout=timeout)
            if r.status_code != 200:
                raise requests.HTTPError(f"HTTP {r.status_code}")
            return cast(Mapping[str, Any], r.json())
        except Exception as e:
            last_exc = e
            if tentativa < max_page_retries:
                espera = (1.5**tentativa) + random.random()
                print(
                    f"[⏳] Tentativa {tentativa+1}/{max_page_retries+1} falhou para {product_id} ({e}); retry em {espera:.1f}s"
                )
                time.sleep(espera)
    # todas as tentativas falharam
    raise TransientPageError(last_exc)


def coletar_vendas(
    product_id: str,
    inicio: str,
    fim: str,
    *,
    tipo_assinatura: str | None = None,
    timeout: tuple[float, float] = (3.0, 15.0),  # (connect, read)
    max_page_retries: int = 2,  # tentativas por página
) -> list[dict[str, Any]]:
    print(f"[🔎 coletar_vendas] Início - Produto: {product_id}, Período: {inicio} → {fim}")

    resultado: list[dict[str, Any]] = []
    cursor: str | None = None
    pagina_count = 0
    total_transacoes = 0
    erro_final = False

    session: Session = requests.Session()

    while True:
        params: dict[str, Any] = {
            "transaction_status[]": ["approved"],
            "ordered_at_ini": inicio,
            "ordered_at_end": fim,
            "product_id": product_id,
        }
        if cursor:
            params["cursor"] = cursor

        try:
            data = _fetch_page_with_retry(
                session,
                base_url=BASE_URL_GURU,
                headers=HEADERS_GURU,
                params=params,
                timeout=timeout,
                max_page_retries=max_page_retries,
                product_id=product_id,
            )
        except TransientPageError as e:
            # se falhou logo na 1ª página e nada coletado → propaga erro transitório
            if pagina_count == 0 and total_transacoes == 0:
                raise TransientGuruError(f"Falha inicial ao buscar transações do produto {product_id}: {e}") from e
            # senão, encerra com parciais
            erro_final = True
            break

        pagina = cast(list[dict[str, Any]], data.get("data", []) or [])
        print(f"[📄 Página {pagina_count+1}] {len(pagina)} vendas encontradas")

        if tipo_assinatura:
            for t in pagina:
                t["tipo_assinatura"] = tipo_assinatura
        resultado.extend(pagina)

        total_transacoes += len(pagina)
        pagina_count += 1

        cursor = cast(str | None, data.get("next_cursor"))
        if not cursor:
            break

    status = "Concluído" if not erro_final else "Concluído (parcial)"
    print(
        f"[✅ coletar_vendas] {status} - Produto {product_id} | Total: {total_transacoes} transações em {pagina_count} página(s)"
    )
    return resultado


def coletar_vendas_com_retry(
    *args: Any,
    tentativas: int = 3,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    for tentativa in range(tentativas):
        try:
            resultado = coletar_vendas(*args, **kwargs)
            return cast(list[dict[str, Any]], resultado)
        except TransientGuruError as e:
            print(f"[⚠️ Retry {tentativa+1}/{tentativas}] {e}")
            if tentativa < tentativas - 1:
                espera = (2**tentativa) + random.random()
                time.sleep(espera)
            else:
                print("[❌] Falhou após retries; retornando vazio.")
                return []
    return []


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
            transacoes, _, dados_final_map = gerenciar_coleta_vendas_assinaturas(
                cast(dict[str, Any], dict(dados)),
                skus_info=skus_info,
            )
        elif modo == "produtos":
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
