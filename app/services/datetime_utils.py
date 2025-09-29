from __future__ import annotations

from datetime import UTC, date, datetime, time as dtime, timedelta
from typing import Any, Optional

# Tenta usar um parser robusto; se não houver, cai para fromisoformat
try:
    from dateutil.parser import parse as parse_date  # type: ignore
except Exception:  # pragma: no cover
    def parse_date(s: str) -> datetime:
        # Fallback simples: ISO estrito
        return datetime.fromisoformat(s)


# ============== FUNÇÕES DE PERÍODO / DATA ==============

def _aware_utc(dt: datetime) -> datetime:
    """Garante datetime com tzinfo=UTC (se vier naïve, aplica UTC; se vier com tz, converte para UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def bimestre_do_mes(mes: int) -> int:
    """Retorna o índice do bimestre (1..6) ao qual o mês pertence."""
    return 1 + (int(mes) - 1) // 2


def _inicio_mes_por_data(dt_in: datetime) -> datetime:
    """Primeiro instante (UTC) do mês referente a dt_in."""
    dt = _aware_utc(dt_in)
    return datetime(dt.year, dt.month, 1, tzinfo=UTC)


def _last_moment_of_month(y: int, m: int) -> datetime:
    """Último instante (UTC) do mês y-m (23:59:59.999999)."""
    if m == 12:
        return datetime(y, 12, 31, 23, 59, 59, 999_999, tzinfo=UTC)
    nxt = datetime(y, m + 1, 1, tzinfo=UTC)
    return nxt - timedelta(microseconds=1)


def _first_day_next_month(dt_in: datetime) -> datetime:
    """Primeiro instante (UTC) do mês seguinte ao de dt_in."""
    dt = _aware_utc(dt_in)
    y, m = dt.year, dt.month
    return datetime(y + (m // 12), 1 if m == 12 else m + 1, 1, tzinfo=UTC)


def _inicio_bimestre_por_data(dt_in: datetime) -> datetime:
    """Primeiro instante (UTC) do bimestre ao qual dt_in pertence.
    Bimestres: (1-2), (3-4), (5-6), (7-8), (9-10), (11-12)
    """
    dt = _aware_utc(dt_in)
    m_ini = dt.month if dt.month % 2 == 1 else dt.month - 1
    return datetime(dt.year, m_ini, 1, tzinfo=UTC)


def _fim_bimestre_por_data(dt_in: datetime) -> datetime:
    """Último instante (UTC) do bimestre ao qual dt_in pertence."""
    dt = _aware_utc(dt_in)
    m_end = dt.month if dt.month % 2 == 0 else dt.month + 1
    y = dt.year
    if m_end == 13:
        y, m_end = y + 1, 1
    return _last_moment_of_month(y, m_end)


def _as_dt(value: str | date | datetime) -> datetime:
    """Converte str/date/datetime para datetime *aware* em UTC."""
    if isinstance(value, datetime):
        return _aware_utc(value)
    if isinstance(value, date):
        return datetime.combine(value, dtime.min, tzinfo=UTC)
    if isinstance(value, str):
        # aceita ISO ou formatos flexíveis via dateutil (quando disponível)
        try:
            dtp = parse_date(value)
        except Exception:
            # tenta ISO estrito (YYYY-MM-DD ou YYYY-MM-DDTHH:MM:SS[…])
            try:
                dtp = datetime.fromisoformat(value)
            except ValueError:
                d = date.fromisoformat(value)
                dtp = datetime.combine(d, dtime.min)
        return _aware_utc(dtp)
    raise TypeError(f"Tipo não suportado: {type(value)!r}")


def _as_iso(d: str | date) -> str:
    """Normaliza uma data em string ISO (YYYY-MM-DD).
    Aceita `date` ou string (YYYY-MM-DD | YYYY-MM-DDTHH:MM[:SS][.ffffff]).
    """
    if isinstance(d, date):
        return d.isoformat()
    try:
        return date.fromisoformat(d).isoformat()
    except ValueError:
        return datetime.fromisoformat(d).date().isoformat()


def _to_dt(val: Any) -> Optional[datetime]:
    """Converte val -> datetime (UTC aware).
    Aceita: datetime | ISO string | timestamp (s/ms) | objetos com .toPyDateTime()
    """
    if val is None:
        return None

    if isinstance(val, datetime):
        return _aware_utc(val)

    if isinstance(val, (int, float)):
        try:
            v = float(val)
            if v > 1e12:  # ms -> s
                v /= 1000.0
            return datetime.fromtimestamp(v, tz=UTC)
        except Exception:
            return None

    if isinstance(val, str):
        try:
            dtp = parse_date(val)
            return _aware_utc(dtp)
        except Exception:
            try:
                dtp = datetime.fromisoformat(val)
                return _aware_utc(dtp)
            except Exception:
                try:
                    d = date.fromisoformat(val)
                    return datetime.combine(d, dtime.min, tzinfo=UTC)
                except Exception:
                    return None

    # Compat: objetos Qt que exponham .toPyDateTime()
    to_py = getattr(val, "toPyDateTime", None)
    if callable(to_py):
        try:
            return _aware_utc(to_py())
        except Exception:
            return None

    return None
