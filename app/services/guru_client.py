# app/services/guru_client.py
from __future__ import annotations

import calendar
import datetime as dt
import random
import time
from collections.abc import Mapping
from threading import Lock, Semaphore
from typing import Any, cast

import requests
from requests import Response, Session

from app.common.settings import settings  # ajuste se seu settings mora aqui

# helper de data (apenas a função de conversão; o resto fica onde já está)
from app.utils.datetime_helpers import _as_dt

UTC = dt.UTC

_GURU_CONC_SEM = Semaphore(getattr(settings, "GURU_MAX_CONCURRENCY", 4))
BASE_URL_GURU = "https://digitalmanager.guru/api/v2"
HEADERS_GURU = {
    "Authorization": f"Bearer {settings.API_KEY_GURU}",
    "Content-Type": "application/json",
}

# Limite inferior de segurança para consultas (ajuste conforme sua regra)
LIMITE_INFERIOR = dt.datetime(2024, 10, 1, tzinfo=UTC)


# ===================== Exceptions =====================


class TransientGuruError(Exception):
    """Erro transitório ao buscar a PRIMEIRA página; recomenda retry externo."""


class TransientPageError(Exception):
    def __init__(self, last_exc: Exception | None):
        super().__init__(str(last_exc) if last_exc else "Falha ao buscar página")
        self.last_exc = last_exc


# ===================== Períodos =====================


def dividir_periodos_coleta_api_guru(
    data_inicio: str | dt.date | dt.datetime,
    data_fim: str | dt.date | dt.datetime,
) -> list[tuple[str, str]]:
    """
    Divide o intervalo em blocos quadrimestrais com fins em abr/ago/dez.
    Retorna lista de pares (YYYY-MM-DD, YYYY-MM-DD). Usa datetime *aware* (UTC).
    """
    ini = _as_dt(data_inicio)
    end = _as_dt(data_fim)
    if not ini.tzinfo:
        ini = ini.replace(tzinfo=UTC)
    if not end.tzinfo:
        end = end.replace(tzinfo=UTC)

    blocos: list[tuple[str, str]] = []
    atual = ini
    while atual <= end:
        ano = atual.year
        mes = atual.month
        # blocos: jan-abr, mai-ago, set-dez
        fim_mes = 4 if mes <= 4 else (8 if mes <= 8 else 12)
        ultimo_dia = calendar.monthrange(ano, fim_mes)[1]
        fim_bloco = dt.datetime(ano, fim_mes, ultimo_dia, 23, 59, 59, tzinfo=UTC)
        fim_bloco = min(fim_bloco, end)

        blocos.append((atual.date().isoformat(), fim_bloco.date().isoformat()))

        # próximo bloco
        proximo_mes = 1 if fim_mes == 12 else fim_mes + 1
        proximo_ano = ano + 1 if fim_mes == 12 else ano
        atual = dt.datetime(proximo_ano, proximo_mes, 1, tzinfo=UTC)

    return blocos


# ===================== HTTP (Guru) =====================


class _RateLimiter:
    def __init__(self, qps: float, burst: int | None = None) -> None:
        self.qps = max(0.1, float(qps))
        self.min_interval = 1.0 / self.qps
        self._lock = Lock()
        self._next_free = 0.0
        self._burst = max(1, int(burst or self.qps * 2))  # janela de estouro “soft”
        self._tokens = self._burst
        self._last = time.monotonic()

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            # recarrega tokens proporcional ao tempo
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self._burst, self._tokens + elapsed * self.qps)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            # sem token: aguarda até próximo slot
            sleep_s = max(self.min_interval, (1.0 - self._tokens) / self.qps)
        time.sleep(sleep_s)


_GURU_RL = _RateLimiter(qps=getattr(settings, "GURU_QPS", 3.0))


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
    # ... (seu código atual de logs permanece)
    url = f"{base_url.rstrip('/')}/transactions"
    hdrs = dict(headers or {})
    hdrs.setdefault("Accept", "application/json")
    for k in list(hdrs.keys()):
        if k.lower() == "content-type":
            hdrs.pop(k, None)

    last_exc: Exception | None = None
    for tentativa in range(max_page_retries + 1):
        try:
            # 🔒 limita concorrência E taxa antes da chamada
            _GURU_CONC_SEM.acquire()
            _GURU_RL.acquire()

            if tentativa == 0:
                print(f"[HTTP] GET {url} pid={product_id} params={params}")

            r: Response = session.get(url, headers=hdrs, params=params, timeout=timeout)
            status = r.status_code
            ct = (r.headers.get("content-type") or "").lower()
            body_text = r.text or ""
            text_sample = body_text[:400].replace("\n", " ").strip()

            print(f"[HTTP] status={status} ct={ct or '-'} pid={product_id}")

            # (restante do tratamento: 204, 429 com Retry-After, 4xx/5xx, json inválido...)
            # ↳ mantenha exatamente como você já colocou no último patch
            # ...
            # sucesso:
            return cast(Mapping[str, Any], r.json())

        except Exception as e:
            last_exc = e
            if tentativa < max_page_retries:
                espera = (1.5**tentativa) + random.random()
                print(
                    f"[⏳ retry {tentativa+1}/{max_page_retries}] pid={product_id} err={e} | aguardando {espera:.1f}s"
                )
                time.sleep(espera)
            else:
                raise TransientPageError(e)
        finally:
            # sempre libera o semáforo (mesmo se der erro)
            try:
                _GURU_CONC_SEM.release()
            except Exception:
                pass


def coletar_vendas(
    product_id: str,
    inicio: str,
    fim: str,
    *,
    tipo_assinatura: str | None = None,
    timeout: tuple[float, float] = (3.0, 15.0),  # (connect, read)
    max_page_retries: int = 2,
) -> list[dict[str, Any]]:
    """Busca transações aprovadas no Guru para um product_id no período informado."""
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
            # Falhou logo na 1ª página e nada coletado → erro transitório, deixe o wrapper decidir
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
    """Wrapper com backoff exponencial para coletar_vendas (erros transitórios)."""
    for tentativa in range(tentativas):
        try:
            return cast(list[dict[str, Any]], coletar_vendas(*args, **kwargs))
        except TransientGuruError as e:
            print(f"[⚠️ Retry {tentativa+1}/{tentativas}] {e}")
            if tentativa < tentativas - 1:
                espera = (2**tentativa) + random.random()
                time.sleep(espera)
            else:
                print("[❌] Falhou após retries; retornando vazio.")
                return []
    return []


__all__ = [
    "BASE_URL_GURU",
    "HEADERS_GURU",
    "LIMITE_INFERIOR",
    "UTC",
    "TransientGuruError",
    "TransientPageError",
    "coletar_vendas",
    "coletar_vendas_com_retry",
    "dividir_periodos_coleta_api_guru",
]
