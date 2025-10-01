import re
import threading
import time
from collections.abc import Iterable
from typing import Any

import requests

# -----------------------------------------------------------------------------
# Enriquecimento: ViaCEP (bairro/logradouro)
# -----------------------------------------------------------------------------
_VIACEP_URL = "https://viacep.com.br/ws/{cep}/json/"
_MIN_INTERVALO_VIACEP = 0.30
_viacep_lock = threading.Lock()
_viacep_ultimo = 0.0


def _limpa_cep(cep: str | None) -> str:
    return re.sub(r"\D", "", str(cep or ""))[:8]


def _viacep_get(cep: str) -> dict[str, Any]:
    global _viacep_ultimo
    cep_limp = _limpa_cep(cep)
    if not cep_limp or len(cep_limp) < 8:
        return {}
    with _viacep_lock:
        delta = time.time() - _viacep_ultimo
        if delta < _MIN_INTERVALO_VIACEP:
            time.sleep(_MIN_INTERVALO_VIACEP - delta)
        _viacep_ultimo = time.time()
    try:
        r = requests.get(_VIACEP_URL.format(cep=cep_limp), timeout=6)
        if r.status_code != 200:
            return {}
        data = r.json()
        if data.get("erro"):
            return {}
        return data
    except Exception:
        return {}


def buscar_cep_com_timeout(cep: str, timeout: float = 6.0) -> dict[str, Any]:
    data = _viacep_get(cep)
    if not data:
        return {}
    return {
        "street": str(data.get("logradouro", "") or ""),
        "district": str(data.get("bairro", "") or ""),
        "city": str(data.get("localidade", "") or ""),
        "uf": str(data.get("uf", "") or ""),
    }


def obter_bairros_por_cep(ceps: Iterable[str]) -> tuple[dict[str, str], dict[str, str]]:
    bairros: dict[str, str] = {}
    logs: dict[str, str] = {}
    for c in ceps:
        cl = _limpa_cep(c)
        if not cl or cl in bairros:
            continue
        d = _viacep_get(cl)
        if d:
            if d.get("bairro"):
                bairros[cl] = str(d["bairro"]).strip()
            if d.get("logradouro"):
                logs[cl] = str(d["logradouro"]).strip()
    return bairros, logs
