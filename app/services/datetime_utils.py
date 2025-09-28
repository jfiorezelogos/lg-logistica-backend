from datetime import date, datetime, timedelta, time as dtime, UTC


# ============== FUNÇÕES DE PERÍODO / DATA ==============
def _aware_utc(dt: datetime) -> datetime:
    """Garante datetime com tzinfo=UTC (se vier naïve, aplica UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def bimestre_do_mes(mes: int) -> int:
    return 1 + (int(mes) - 1) // 2


def _inicio_mes_por_data(dt: datetime) -> datetime:
    dt = _aware_utc(dt)
    return datetime(dt.year, dt.month, 1, tzinfo=UTC)


def _last_moment_of_month(y: int, m: int) -> datetime:
    if m == 12:
        return datetime(y, 12, 31, 23, 59, 59, 999_999, tzinfo=UTC)
    nxt = datetime(y, m + 1, 1, tzinfo=UTC)
    return nxt - timedelta(microseconds=1)

def _first_day_next_month(dt: datetime) -> datetime:
    dt = _aware_utc(dt)
    y, m = dt.year, dt.month
    return datetime(y + (m // 12), 1 if m == 12 else m + 1, 1, tzinfo=UTC)

def _inicio_bimestre_por_data(dt: datetime) -> datetime:
    dt = _aware_utc(dt)
    # bimestres: (1-2), (3-4), (5-6), (7-8), (9-10), (11-12)
    m_ini = dt.month if dt.month % 2 == 1 else dt.month - 1
    return datetime(dt.year, m_ini, 1, tzinfo=UTC)


def _fim_bimestre_por_data(dt: datetime) -> datetime:
    dt = _aware_utc(dt)
    m_end = dt.month if dt.month % 2 == 0 else dt.month + 1
    y = dt.year
    if m_end == 13:
        y, m_end = y + 1, 1
    return _last_moment_of_month(y, m_end)


def _as_dt(value: str | date | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, dtime.min)
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            d = date.fromisoformat(value)
            return datetime.combine(d, dtime.min)
    raise TypeError(f"Tipo não suportado: {type(value)!r}")