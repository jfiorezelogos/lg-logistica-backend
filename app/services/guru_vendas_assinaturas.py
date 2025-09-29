# app/services/assinaturas.py

# ============== IMPORTS ==============
from __future__ import annotations

import calendar
import datetime as dt

# stdlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol, TypedDict, cast

# terceiros
from unidecode import unidecode

from app.common.settings import settings
from app.services.guru_client import LIMITE_INFERIOR, coletar_vendas_com_retry, dividir_periodos_coleta_api_guru
from app.services.loader_produtos_info import produto_indisponivel
from app.utils.datetime_helpers import (
    UTC,
    _as_dt,
    _fim_bimestre_por_data,
    _first_day_next_month,
    _inicio_bimestre_por_data,
    _inicio_mes_por_data,
    _last_moment_of_month,
    _to_dt,
    bimestre_do_mes,
)


# ============== TIPAGEM AUXILIAR ==============
class HasIsSet(Protocol):
    def is_set(self) -> bool: ...


# ============== CONSTANTES GERAIS ==============


def calcular_periodo_assinatura(ano: int, mes: int, periodicidade: str) -> tuple[dt.datetime, dt.datetime, int]:
    periodicidade = (periodicidade or "").strip().lower()

    if periodicidade == "mensal":
        dt_ini = dt.datetime(ano, mes, 1, 0, 0, 0, tzinfo=UTC)
        last_day = calendar.monthrange(ano, mes)[1]
        dt_end = dt.datetime(ano, mes, last_day, 23, 59, 59, tzinfo=UTC)
        periodo = mes
    else:  # bimestral
        bim = bimestre_do_mes(mes)
        m1 = 1 + (bim - 1) * 2
        m2 = m1 + 1
        dt_ini = dt.datetime(ano, m1, 1, 0, 0, 0, tzinfo=UTC)
        last_day = calendar.monthrange(ano, m2)[1]
        dt_end = dt.datetime(ano, m2, last_day, 23, 59, 59, tzinfo=UTC)
        periodo = bim

    return dt_ini, dt_end, periodo


# ============== SKUs / REGRAS =============


def ler_regras_assinaturas(config_path: str | None = None) -> list[dict[str, Any]]:
    try:
        if not config_path:
            config_path = os.path.join(os.path.dirname(__file__), "config_ofertas.json")
        path = Path(config_path)
        if path.exists():
            with path.open(encoding="utf-8") as f:
                cfg: dict[str, Any] = json.load(f)
                regras = cfg.get("rules") or cfg.get("regras") or []
                if isinstance(regras, list):
                    return regras
    except Exception:
        pass
    return []


def mapear_periodicidade_assinaturas(
    skus_info: Mapping[str, Mapping[str, Any]],
    periodicidade_sel: str,
) -> dict[str, list[str]]:
    """
    Retorna dict com listas de product_ids (Guru) das assinaturas filtradas pela periodicidade
    ('mensal' | 'bimestral').

    Keys: 'anuais', 'bianuais', 'trianuais', 'bimestrais', 'mensais', 'todos'
    """
    periodicidade_sel = (periodicidade_sel or "").strip().lower()
    mapa_tipo: dict[str, str] = {
        "anual": "anuais",
        "bianual": "bianuais",
        "trianual": "trianuais",
        "bimestral": "bimestrais",
        "mensal": "mensais",
    }

    ids_por_tipo: dict[str, list[str]] = {k: [] for k in ["anuais", "bianuais", "trianuais", "bimestrais", "mensais"]}
    todos: set[str] = set()

    for _nome, info in skus_info.items():
        if str(info.get("tipo", "")).lower() != "assinatura":
            continue
        if str(info.get("periodicidade", "")).lower() != periodicidade_sel:
            continue

        duracao = str(info.get("recorrencia", "")).lower()
        chave_tipo = mapa_tipo.get(duracao)
        if not chave_tipo:
            continue

        guru_ids: Sequence[Any] = cast(Sequence[Any], info.get("guru_ids", []))
        for gid in guru_ids:
            gid_str = str(gid).strip()
            if gid_str:
                ids_por_tipo[chave_tipo].append(gid_str)
                todos.add(gid_str)

    # dedup (mantém ordem de inserção)
    for k in list(ids_por_tipo.keys()):
        ids_por_tipo[k] = list(dict.fromkeys(ids_por_tipo[k]))
    ids_por_tipo["todos"] = list(todos)
    return ids_por_tipo


# ============== GERENCIAR COLETA (ASSINATURAS) ==============
def gerenciar_coleta_vendas_assinaturas(
    dados: dict[str, Any],
    *,
    skus_info: Mapping[str, Mapping[str, Any]],
    atualizar: Callable[[str, int, int], Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    print("[🔍 gerenciar_coleta_vendas_assinaturas] Início da função")

    transacoes: list[dict[str, Any]] = []
    transacoes_com_erro: list[str] = []

    # contexto
    periodicidade_sel: str = (
        (str(dados.get("periodicidade") or dados.get("periodicidade_selecionada") or "")).strip().lower()
    )
    if periodicidade_sel not in ("mensal", "bimestral"):
        periodicidade_sel = "bimestral"

    # IDs por periodicidade a partir do skus_info (argumento)
    ids_map: dict[str, list[str]] = mapear_periodicidade_assinaturas(skus_info, periodicidade_sel)
    dados["ids_planos_todos"] = ids_map.get("todos", [])

    # período
    dt_ini_sel: dt.datetime | None = (
        dados.get("ordered_at_ini_periodo")
        or dados.get("ordered_at_ini_anual")
        or dados.get("ordered_at_ini_bimestral")
    )
    dt_end_sel: dt.datetime | None = (
        dados.get("ordered_at_end_periodo")
        or dados.get("ordered_at_end_anual")
        or dados.get("ordered_at_end_bimestral")
    )
    if not dt_ini_sel or not dt_end_sel:
        raise ValueError("ordered_at_ini / ordered_at_end não informados para o período selecionado.")

    # normaliza período selecionado
    end_sel = _as_dt(dt_end_sel)
    if periodicidade_sel == "mensal":
        ini_sel = _inicio_mes_por_data(end_sel)
        end_sel = _last_moment_of_month(end_sel.year, end_sel.month)
    else:  # bimestral
        ini_sel = _inicio_bimestre_por_data(end_sel)
        end_sel = _fim_bimestre_por_data(end_sel)

    # intervalos
    intervalos_mensais: list[tuple[str, str]] = (
        dividir_periodos_coleta_api_guru(ini_sel, end_sel) if periodicidade_sel == "mensal" else []
    )
    intervalos_bimestrais: list[tuple[str, str]] = (
        dividir_periodos_coleta_api_guru(ini_sel, end_sel) if periodicidade_sel == "bimestral" else []
    )

    # janelas multi-ano
    inicio_base = _first_day_next_month(end_sel)

    def _janela_multi_ano(n_anos: int) -> list[tuple[str, str]]:
        ini = dt.datetime(inicio_base.year - n_anos, inicio_base.month, 1, tzinfo=UTC)
        ini = max(ini, LIMITE_INFERIOR)
        return cast(list[tuple[str, str]], dividir_periodos_coleta_api_guru(ini, end_sel))

    # modo do período
    try:
        modo_sel_norm = unidecode((dados.get("modo_periodo") or "").strip().upper())
    except Exception:
        modo_sel_norm = (dados.get("modo_periodo") or "").strip().upper().replace("Í", "I").replace("É", "E")

    if modo_sel_norm == "PERIODO":
        intervalos_anuais = dividir_periodos_coleta_api_guru(ini_sel, end_sel)
        intervalos_bianuais = dividir_periodos_coleta_api_guru(ini_sel, end_sel)
        intervalos_trianuais = dividir_periodos_coleta_api_guru(ini_sel, end_sel)
    else:
        intervalos_anuais = _janela_multi_ano(1)
        intervalos_bianuais = _janela_multi_ano(2)
        intervalos_trianuais = _janela_multi_ano(3)

    def executar_lote(
        tarefas: Sequence[tuple[str, str, str, str]],
        label_progresso: str,
    ) -> bool:
        if not tarefas:
            return True
        max_workers = min(getattr(settings, "GURU_MAX_CONCURRENCY", 4), len(tarefas))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    coletar_vendas_com_retry,
                    pid,
                    ini,
                    fim,
                    tipo_assinatura=tipo_ass,
                )
                for (pid, ini, fim, tipo_ass) in tarefas
            ]
            total_futures = len(futures)
            concluidos = 0
            while futures:
                done, not_done = wait(futures, timeout=0.5, return_when=FIRST_COMPLETED)
                for future in done:
                    try:
                        resultado = future.result()
                        transacoes.extend(cast(list[dict[str, Any]], resultado))
                    except Exception as e:
                        erro_msg = f"Erro ao buscar transações ({label_progresso}): {e!s}"
                        print(f"❌ {erro_msg}")
                        transacoes_com_erro.append(erro_msg)
                    finally:
                        concluidos += 1
                        if atualizar:
                            with suppress(Exception):
                                atualizar(f"🔄 {label_progresso}", concluidos, total_futures)
                futures = list(not_done)
        return True

    # tarefas agregadas
    todas_tarefas: list[tuple[str, str, str, str]] = []

    print("[1️⃣] Gerando tarefas para anuais...")
    t = [(pid, ini, fim, "anuais") for pid in ids_map.get("anuais", []) for (ini, fim) in intervalos_anuais]
    todas_tarefas.extend(t)

    print("[1.1️⃣] Gerando tarefas para bianuais...")
    t = [(pid, ini, fim, "bianuais") for pid in ids_map.get("bianuais", []) for (ini, fim) in intervalos_bianuais]
    todas_tarefas.extend(t)

    print("[1.2️⃣] Gerando tarefas para trianuais...")
    t = [(pid, ini, fim, "trianuais") for pid in ids_map.get("trianuais", []) for (ini, fim) in intervalos_trianuais]
    todas_tarefas.extend(t)

    print("[2️⃣] Gerando tarefas para bimestrais...]")
    t = [(pid, ini, fim, "bimestrais") for pid in ids_map.get("bimestrais", []) for (ini, fim) in intervalos_bimestrais]
    todas_tarefas.extend(t)

    print("[3️⃣] Gerando tarefas para mensais...")
    t = [(pid, ini, fim, "mensais") for pid in ids_map.get("mensais", []) for (ini, fim) in intervalos_mensais]
    todas_tarefas.extend(t)

    # executa tudo de uma vez
    total_tarefas = len(todas_tarefas)
    print(f"[🧵] Disparando {total_tarefas} tarefas no executor único...")

    if total_tarefas == 0:
        print("[INFO] Nenhuma tarefa gerada para o período/periodicidade selecionados.")
        print(f"[✅ gerenciar_coleta_vendas_assinaturas] Finalizado - {len(transacoes)} transações")
        return transacoes, {}, dados

    ok = executar_lote(todas_tarefas, "Coletando transações...")
    if not ok:
        print("[⛔] Execução interrompida.")
        return transacoes, {}, dados

    print(f"[✅ gerenciar_coleta_vendas_assinaturas] Finalizado - {len(transacoes)} transações")
    return transacoes, {}, dados


# ============== CRIAR PLANILHA (ASSINATURAS) ==============


def _norm(s: str) -> str:
    return unidecode((s or "").strip().lower())


class AplicarRegrasAssinaturas(TypedDict, total=False):
    override_box: str | None
    brindes_extra: list[dict[str, Any]]


def aplicar_regras_assinaturas(
    transacao: Mapping[str, Any],
    dados: Mapping[str, Any],
    _skus_info: Mapping[str, Any],
    base_produto_principal: str,
) -> AplicarRegrasAssinaturas:
    """Lê config_ofertas.json e aplica:

      - override da box (action.type == 'alterar_box')
      - brindes extras (action.type == 'adicionar_brindes')

    Compatível com rótulos do JSON como:
      "Assinatura 2 anos (bimestral)", "Assinatura Anual (mensal)",
      "Assinatura Bimestral (bimestral)" etc.
    Sem mudar o JSON.
    """
    regras_payload = cast(Sequence[Mapping[str, Any]], (dados.get("rules") or []))
    regras = regras_payload or []
    res_override: str | None = None
    res_override_score: int = -1
    brindes_raw: list[dict[str, Any] | str] = []

    # --- contexto da transação ---
    payment: Mapping[str, Any] = transacao.get("payment") or {}
    coupon: Mapping[str, Any] = payment.get("coupon") or {}
    coupon_code_norm: str = _norm(str(coupon.get("coupon_code") or ""))

    tipo_ass: str = str(transacao.get("tipo_assinatura") or "").strip().lower()  # anuais, bianuais, ...
    periodicidade: str = str(dados.get("periodicidade_selecionada") or dados.get("periodicidade") or "").strip().lower()

    # Mapeia tipo_ass + periodicidade -> rótulos usados no JSON
    def _labels_assinatura(tipo: str, per: str) -> set[str]:
        # exemplos no JSON:
        # "Assinatura 2 anos (bimestral)", "Assinatura 3 anos (mensal)",
        # "Assinatura Anual (bimestral)", "Assinatura Bimestral (bimestral)"
        base: list[str] = []
        if tipo == "bianuais":
            base.append("Assinatura 2 anos")
        elif tipo == "trianuais":
            base.append("Assinatura 3 anos")
        elif tipo == "anuais":
            base.append("Assinatura Anual")
        elif tipo == "bimestrais":
            base.append("Assinatura Bimestral")
        elif tipo == "mensais":
            base.append("Assinatura Mensal")
        out: set[str] = set()
        for b in base or ["Assinatura"]:
            out.add(f"{b} ({per})" if per else b)
        return {_norm(x) for x in out}

    labels_alvo: set[str] = _labels_assinatura(tipo_ass, periodicidade)
    base_prod_norm: str = _norm(base_produto_principal)

    def _assinatura_match(lista: Sequence[str] | None) -> tuple[bool, int]:
        """Retorna (casou?, score). Score maior = mais específico.

        Regras:
          - lista vazia => aplica (score 0)
          - se qualquer item da lista bate exatamente com um dos rótulos conhecidos -> score 3
          - se item corresponde ao nome do box atual -> score 2
          - se item contém tokens genéricos (anual / 2 anos / 3 anos / mensal / bimestral) presentes no rótulo -> score 1
        """
        if not lista:
            return True, 0

        tokens_genericos = {"anual", "2 anos", "3 anos", "mensal", "bimestral"}
        best = -1
        casou = False
        alvo_concat = " ".join(sorted(labels_alvo))

        for it in lista:
            itn = _norm(it or "")
            if not itn:
                casou, best = True, max(best, 0)
                continue
            if itn in labels_alvo:
                casou, best = True, max(best, 3)
                continue
            if itn == base_prod_norm:
                casou, best = True, max(best, 2)
                continue
            if itn in tokens_genericos and itn in alvo_concat:
                casou, best = True, max(best, 1)

        return casou, (best if best >= 0 else -1)

    for r in regras:
        if str(r.get("applies_to") or "").strip().lower() != "cupom":
            continue

        cupom_cfg: Mapping[str, Any] = r.get("cupom") or {}
        alvo_cupom: str = _norm(str(cupom_cfg.get("nome") or ""))
        if not alvo_cupom or alvo_cupom != coupon_code_norm:
            continue

        assinaturas_lista = r.get("assinaturas") or []
        ok, score = _assinatura_match(assinaturas_lista)
        if not ok:
            continue

        action: Mapping[str, Any] = r.get("action") or {}
        atype = str(action.get("type") or "").strip().lower()

        if atype == "adicionar_brindes":
            # pode vir lista de strings ou de objetos
            items = action.get("brindes") or []
            if isinstance(items, list):
                for b in items:
                    if isinstance(b, dict | str):
                        brindes_raw.append(b)

        elif atype == "alterar_box":
            box = str(action.get("box") or "").strip()
            if box and score > res_override_score:
                res_override = box
                res_override_score = score

    # Normalização final: remove duplicatas e ignora iguais ao box atual/override.
    override_norm = _norm(res_override or base_produto_principal)
    uniq: list[dict[str, Any]] = []
    seen: set[str] = set()

    for b in brindes_raw:
        if isinstance(b, dict):
            nb = str(b.get("nome", "")).strip()
            payload: dict[str, Any] = dict(b)
            if not nb:
                # se não veio 'nome', tenta usar 'nome' a partir de outra chave, senão pula
                continue
        else:
            nb = b.strip()
            if not nb:
                continue
            payload = {"nome": nb}

        nbn = _norm(nb)
        if nbn in (base_prod_norm, override_norm):
            continue
        if nbn in seen:
            continue

        seen.add(nbn)
        uniq.append(payload)

    return AplicarRegrasAssinaturas(override_box=res_override, brindes_extra=uniq)


def validar_regras_assinatura(dados: dict, data_pedido: dt.datetime) -> bool:
    """
    Retorna True se a data do pedido estiver dentro do período da assinatura,
    permitindo aplicar regras de ofertas/cupons configuradas.

    - NÃO aplica para modo 'produtos'.
    - Usa ordered_at_ini_periodo/ordered_at_end_periodo se existirem; senão, deriva via calcular_periodo_assinatura.
    - Converte TUDO para dt.datetime *aware* (UTC) antes de comparar.
    - Logs defensivos sem referenciar variáveis ainda não definidas.
    """

    try:
        if not isinstance(dados, dict):
            return False

        modo_local = (str(dados.get("modo") or dados.get("modo_busca") or "")).strip().lower()
        if modo_local == "produtos":
            return False

        # 0) normaliza a data da transação ANTES de qualquer print/comparação
        dp = _to_dt(data_pedido)
        if dp is None:
            print(f"[DEBUG dentro_periodo] data_pedido inválido: {data_pedido!r}")
            return False

        # 1) tenta janela explícita
        ini = _to_dt(dados.get("ordered_at_ini_periodo"))
        end = _to_dt(dados.get("ordered_at_end_periodo"))

        # 2) deriva via ano/mês/periodicidade, se necessário
        if ini is None or end is None:
            ano_s = dados.get("ano")
            mes_s = dados.get("mes")
            periodicidade = (str(dados.get("periodicidade") or "bimestral")).strip().lower()

            if ano_s is None or mes_s is None:
                print(f"[DEBUG dentro_periodo] sem contexto suficiente (ano={ano_s}, mes={mes_s})")
                return False

            try:
                ano_i = int(ano_s)
                mes_i = int(mes_s)
            except Exception:
                print(f"[DEBUG dentro_periodo] sem contexto suficiente (ano={ano_s}, mes={mes_s})")
                return False

            try:
                ini_calc, end_calc, _ = calcular_periodo_assinatura(ano_i, mes_i, periodicidade)
            except Exception as e:
                print(f"[DEBUG janela-skip] calcular_periodo_assinatura erro: {e}")
                return False

            ini = _to_dt(ini_calc)
            end = _to_dt(end_calc)

        if ini is None or end is None:
            print(f"[DEBUG dentro_periodo] janela inválida ini={ini!r} end={end!r}")
            return False

        # Log consolidado (agora com TUDO definido)
        print(f"[DEBUG dentro_periodo] dp={dp} ini={ini} end={end}")

        # 3) comparação segura (todos UTC aware)
        try:
            return ini <= dp <= end
        except Exception as e:
            print(
                f"[DEBUG dentro_periodo] comparação falhou: {type(e).__name__}: {e} "
                f"(types: ini={type(ini)}, dp={type(dp)}, end={type(end)})"
            )
            return False

    except Exception as e:
        print(f"[DEBUG janela-skip] {type(e).__name__}: {e}")
        return False


# ============== PAYLOAD (equivalente ao início de iniciar_busca_assinaturas) ==============


class BoxIndisponivelError(Exception):
    """Erro para indicar que o box selecionado está indisponível no SKUs."""


def montar_payload_busca_assinaturas(
    *,
    ano: int,
    mes: int,
    modo_periodo: str,  # "PERÍODO" | "TODAS"
    box_nome: str | None,
    periodicidade: str,  # "mensal" | "bimestral" (default bimestral)
    skus_info: Mapping[str, Mapping[str, Any]] | None = None,
    rules_path: str | None = None,
) -> dict[str, Any]:
    try:
        ano_i = int(ano)
        mes_i = int(mes)
    except Exception:
        raise ValueError(f"ano/mes inválidos: ano={ano!r} mes={mes!r}")

    if not (1 <= mes_i <= 12):
        raise ValueError(f"mês inválido: {mes}")
    if ano_i < 1900:
        raise ValueError(f"ano inválido: {ano}")

    per = (periodicidade or "").strip().lower()
    if per not in ("mensal", "bimestral"):
        per = "bimestral"

    dt_ini, dt_end, periodo = calcular_periodo_assinatura(ano_i, mes_i, per)

    modo = (modo_periodo or "").strip().upper()
    if modo == "PERIODO":
        modo = "PERÍODO"
    if modo not in ("PERÍODO", "TODAS"):
        modo = "PERÍODO"

    box = (box_nome or "").strip()
    if box and produto_indisponivel(box, skus_info=skus_info):
        raise BoxIndisponivelError(f'Box indisponível: "{box}"')

    regras = ler_regras_assinaturas(rules_path)
    if not isinstance(regras, list):
        regras = []

    return {
        "modo": "assinaturas",
        "ano": ano_i,
        "mes": mes_i,
        "periodicidade": per,
        "periodo": int(periodo),
        "ordered_at_ini_periodo": dt_ini,
        "ordered_at_end_periodo": dt_end,
        "ordered_at_ini_periodo_iso": dt_ini.isoformat(),
        "ordered_at_end_periodo_iso": dt_end.isoformat(),
        "box_nome": box,
        "rules": regras,
        "embutido_ini_ts": dt_ini.timestamp(),
        "embutido_end_ts": dt_end.timestamp(),
        "modo_periodo": modo,
    }
