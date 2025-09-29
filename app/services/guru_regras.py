# app/services/regras_service.py
from __future__ import annotations

import json
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import requests

# Reúso do cliente/constantes do Guru (sem UI)
from app.services.guru_client import BASE_URL_GURU, HEADERS_GURU

# ======================== I/O de regras (arquivo) ========================


def carregar_regras(config_path: str | Path) -> list[dict[str, Any]]:
    """
    Lê a lista de regras do arquivo JSON (config_ofertas.json).
    Se o arquivo não existir, retorna [].
    """
    p = Path(config_path)
    if not p.exists():
        return []
    try:
        with p.open(encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        raise ValueError(f"JSON inválido em {p}: {e}") from e

    # aceita formatos { "rules": [...] } ou lista direta [...]
    if isinstance(data, dict) and isinstance(data.get("rules"), list):
        return cast(list[dict[str, Any]], data["rules"])
    if isinstance(data, list):
        return cast(list[dict[str, Any]], data)
    return []


def salvar_regras(config_path: str | Path, rules: Sequence[Mapping[str, Any]]) -> None:
    """
    Persiste as regras no arquivo (formato { "rules": [...] }) de forma atômica.
    """
    p = Path(config_path)
    payload = {"rules": list(rules)}
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        tmp.replace(p)  # operação atômica
    finally:
        if tmp.exists():
            try:
                tmp.unlink(missing_ok=True)  # py>=3.8
            except Exception:
                pass


# ======================== Coleta de produtos (Guru) ========================


def coletar_produtos_guru(*, limit: int = 100, timeout: float = 10.0) -> list[dict[str, Any]]:
    """
    Busca TODOS os produtos do Guru em páginas (cursor) e retorna list[dict].
    """
    url = f"{BASE_URL_GURU}/products"
    headers = dict(HEADERS_GURU)
    headers.setdefault("Accept", "application/json")

    produtos: list[dict[str, Any]] = []
    cursor: str | None = None
    pagina = 1

    while True:
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        r = requests.get(url, headers=headers, params=params, timeout=timeout)
        if r.status_code != 200:
            raise RuntimeError(f"[Guru] HTTP {r.status_code} ao buscar produtos: {r.text}")

        data = cast(dict[str, Any], r.json())
        pagina_dados = cast(list[dict[str, Any]], data.get("data", []) or [])
        print(f"[📄 Guru] Página {pagina}: {len(pagina_dados)} produtos")

        produtos.extend(pagina_dados)
        cursor = cast(str | None, data.get("next_cursor"))
        if not cursor:
            break
        pagina += 1

    print(f"[✅ Guru] Total de produtos carregados: {len(produtos)}")
    return produtos


# ======================== Backend do “iniciar_gerenciador_regras” ========================


def iniciar_gerenciador_regras_backend(
    *,
    estado: MutableMapping[str, Any] | None = None,
    skus_info: Any = None,  # mantido para compat, mas não usado aqui
    config_path: str | Path,
) -> dict[str, Any]:
    """
    Equivalente backend do iniciar_gerenciador_regras:
      - carrega produtos do Guru
      - carrega regras do arquivo
      - retorna um “contexto” simples para o consumidor (router/serviço).
    """
    ctx: dict[str, Any] = {}
    try:
        ctx["produtos_guru"] = coletar_produtos_guru()
    except Exception as e:
        print(f"[❌ Guru] Exceção ao buscar produtos: {e}")
        ctx["produtos_guru"] = []

    ctx["rules"] = carregar_regras(config_path)
    ctx["config_path"] = str(config_path)
    # “estado” externo pode ser atualizado pelo chamador, mas não é obrigatório
    if isinstance(estado, dict):
        estado["rules"] = ctx["rules"]
        estado["produtos_guru"] = ctx["produtos_guru"]
    return ctx


# ======================== Operações sobre regras (sem UI) ========================


def gerar_uuid() -> str:
    return str(uuid4())


def add_regra(rules: list[dict[str, Any]], regra: Mapping[str, Any]) -> list[dict[str, Any]]:
    """
    Adiciona uma regra ao final da lista (gera id se não houver).
    """
    nova = dict(regra)
    nova.setdefault("id", gerar_uuid())
    rules.append(nova)
    return rules


def edit_regra(rules: list[dict[str, Any]], idx: int, regra: Mapping[str, Any]) -> list[dict[str, Any]]:
    """
    Substitui a regra no índice informado (mantém id existente, se houver).
    """
    if not 0 <= idx < len(rules):
        raise IndexError("Índice de regra inválido")
    atual = dict(regra)
    atual.setdefault("id", rules[idx].get("id") or gerar_uuid())
    rules[idx] = atual
    return rules


def dup_regra(rules: list[dict[str, Any]], idx: int) -> list[dict[str, Any]]:
    """
    Duplica a regra no índice, gerando novo id, e insere logo abaixo.
    """
    if not 0 <= idx < len(rules):
        raise IndexError("Índice de regra inválido")
    copia = json.loads(json.dumps(rules[idx]))  # deep copy
    copia["id"] = gerar_uuid()
    rules.insert(idx + 1, copia)
    return rules


def del_regra(rules: list[dict[str, Any]], idx: int) -> list[dict[str, Any]]:
    """
    Remove a regra no índice.
    """
    if not 0 <= idx < len(rules):
        raise IndexError("Índice de regra inválido")
    del rules[idx]
    return rules


def move_relative_in_group(rules: list[dict[str, Any]], idx_global: int, delta: int) -> list[dict[str, Any]]:
    """
    Move a regra idx_global para cima/baixo apenas trocando com vizinhos do MESMO grupo (applies_to).
    delta: -1 (subir) | +1 (descer)
    """
    if not -1 <= delta <= 1 or delta == 0:
        return rules
    if not 0 <= idx_global < len(rules):
        raise IndexError("Índice de regra inválido")

    group = (rules[idx_global].get("applies_to") or "oferta").strip().lower()
    j = idx_global + delta
    while 0 <= j < len(rules) and (rules[j].get("applies_to") or "oferta").strip().lower() != group:
        j += delta

    if 0 <= j < len(rules):
        rules[idx_global], rules[j] = rules[j], rules[idx_global]
    return rules
