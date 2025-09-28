# app/services/assinaturas.py

# ============== IMPORTS ==============
from __future__ import annotations

# stdlib
import json
import os
import pandas as pd
import random
import time
import calendar
from contextlib import suppress
from collections import defaultdict
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED
from typing import Any, Mapping, Optional, Sequence, Callable, Protocol, cast, TypedDict
import datetime as dt
from common.settings import settings
from app.services.datetime_utils import bimestre_do_mes, _as_dt, _inicio_mes_por_data, _inicio_bimestre_por_data, _last_moment_of_month, _fim_bimestre_por_data, _first_day_next_month
from dateutil.parser import parse as parse_date
# terceiros
import requests
from requests import Session, Response
from unidecode import unidecode


# (opcional) correlação p/ logs — importado, mas ainda não usado aqui
from app.services.logging_utils import set_correlation_id, get_correlation_id  # noqa: F401

# ============== TIPAGEM AUXILIAR ==============
class HasIsSet(Protocol):
    def is_set(self) -> bool: ...


# ============== CONSTANTES GERAIS ==============
UTC = dt.timezone.utc

BASE_URL_GURU = "https://digitalmanager.guru/api/v2"
HEADERS_GURU = {
    "Authorization": f"Bearer {settings.API_KEY_GURU}",
    "Content-Type": "application/json",
}

LIMITE_INFERIOR = dt.datetime(2024, 10, 1, tzinfo=UTC)


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

        if fim_bloco > end:
            fim_bloco = end

        blocos.append((atual.date().isoformat(), fim_bloco.date().isoformat()))

        # próximo bloco
        proximo_mes = fim_mes + 1
        proximo_ano = ano
        if proximo_mes > 12:
            proximo_mes = 1
            proximo_ano += 1
        atual = dt.datetime(proximo_ano, proximo_mes, 1, tzinfo=UTC)

    return blocos


# ============== SKUs / REGRAS ==============
def produto_indisponivel(
    produto_nome: str,
    *,
    skus_info: Optional[Mapping[str, Mapping[str, Any]]] = None,
    sku: str | None = None,
) -> bool:
    if not produto_nome and not sku:
        return False

    skus: Mapping[str, Mapping[str, Any]] = skus_info or {}
    info: Mapping[str, Any] | None = skus.get(produto_nome)

    # fallback por normalização do nome
    if info is None and produto_nome:
        alvo = unidecode(str(produto_nome)).lower().strip()
        for nome, i in skus.items():
            if unidecode(nome).lower().strip() == alvo:
                info = i
                break

    # fallback por SKU
    if info is None and sku:
        sku_norm = (sku or "").strip().upper()
        for i in skus.values():
            if str(i.get("sku", "")).strip().upper() == sku_norm:
                info = i
                break

    return bool(info and info.get("indisponivel", False))


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

    ids_por_tipo: dict[str, list[str]] = {
        k: [] for k in ["anuais", "bianuais", "trianuais", "bimestrais", "mensais"]
    }
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


# ============== COLETA NO GURU (HTTP) ==============
class TransientGuruError(Exception):
    """Erro transitório ao buscar a PRIMEIRA página; deve acionar retry externo."""

def coletar_vendas(
    product_id: str,
    inicio: str,
    fim: str,
    *,
    tipo_assinatura: str | None = None,
    timeout: tuple[float, float] = (3.0, 15.0),  # (connect, read)
    max_page_retries: int = 2,                   # tentativas por página
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

        data: Mapping[str, Any] | None = None
        last_exc: Exception | None = None

        # tentativas por página
        for tentativa in range(max_page_retries + 1):
            try:
                r: Response = session.get(
                    f"{BASE_URL_GURU}/transactions",
                    headers=HEADERS_GURU,
                    params=params,
                    timeout=timeout,
                )
                if r.status_code != 200:
                    raise requests.HTTPError(f"HTTP {r.status_code}")
                data = cast(Mapping[str, Any], r.json())
                break  # sucesso
            except Exception as e:
                last_exc = e
                if tentativa < max_page_retries:
                    espera = (1.5**tentativa) + random.random()
                    print(
                        f"[⏳] Tentativa {tentativa+1}/{max_page_retries+1} falhou para {product_id} ({e}); novo retry em {espera:.1f}s"
                    )
                    time.sleep(espera)
                else:
                    print(f"❌ Falha ao obter página para {product_id} após {max_page_retries+1} tentativas: {e}")

        # Se não conseguiu obter esta página:
        if data is None:
            if pagina_count == 0 and total_transacoes == 0:
                # falhou logo de cara → deixa o wrapper decidir (retry externo)
                raise TransientGuruError(f"Falha inicial ao buscar transações do produto {product_id}: {last_exc}")
            else:
                # falhou depois de já ter coletado algo → devolve parciais
                erro_final = True
                break

        pagina = cast(list[dict[str, Any]], data.get("data", []) or [])
        print(f"[📄 Página {pagina_count+1}] {len(pagina)} vendas encontradas")

        for t in pagina:
            if tipo_assinatura:
                t["tipo_assinatura"] = tipo_assinatura
            resultado.append(t)

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
        max_workers = min(12, len(tarefas))
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

def padronizar_planilha_bling(df: pd.DataFrame, preservar_extras: bool = True) -> pd.DataFrame:
    colunas_padrao = [
        "Número pedido",
        "Nome Comprador",
        "Data",
        "Data Pedido",
        "CPF/CNPJ Comprador",
        "Endereço Comprador",
        "Bairro Comprador",
        "Número Comprador",
        "Complemento Comprador",
        "CEP Comprador",
        "Cidade Comprador",
        "UF Comprador",
        "Telefone Comprador",
        "Celular Comprador",
        "E-mail Comprador",
        "Produto",
        "SKU",
        "Un",
        "Quantidade",
        "Valor Unitário",
        "Valor Total",
        "Total Pedido",
        "Valor Frete Pedido",
        "Valor Desconto Pedido",
        "Outras despesas",
        "Nome Entrega",
        "Endereço Entrega",
        "Número Entrega",
        "Complemento Entrega",
        "Cidade Entrega",
        "UF Entrega",
        "CEP Entrega",
        "Bairro Entrega",
        "Transportadora",
        "Serviço",
        "Tipo Frete",
        "Observações",
        "Qtd Parcela",
        "Data Prevista",
        "Vendedor",
        "Forma Pagamento",
        "ID Forma Pagamento",
        "transaction_id",
        "subscription_id",
        "product_id",
        "Plano Assinatura",
        "Cupom",
        "periodicidade",
        "periodo",
        # 👇 importantes p/ pipeline
        "indisponivel",  # mantemos a marcação feita na coleta
        "ID Lote",  # será preenchido no aplicar_lotes
    ]

    df_out = df.copy()

    # garante todas as colunas padrão
    for coluna in colunas_padrao:
        if coluna not in df_out.columns:
            df_out[coluna] = ""

    # reordena pelas padrão
    base = df_out[colunas_padrao]

    if not preservar_extras:
        return base

    # preserva quaisquer colunas extras ao final (na ordem atual)
    extras = [c for c in df_out.columns if c not in colunas_padrao]
    if extras:
        return pd.concat([base, df_out[extras]], axis=1)

    return base

def gerar_linha_base_planilha(
    contact: Mapping[str, Any],
    valores: Mapping[str, Any],
    transacao: Mapping[str, Any],
    tipo_plano: str = "",
    subscription_id: str = "",
    cupom_valido: str = "",
) -> dict[str, Any]:
    telefone = contact.get("phone_number", "")
    return {
        # Comprador
        "Nome Comprador": contact.get("name", ""),
        "Data Pedido": valores["data_pedido"].strftime("%d/%m/%Y"),
        "Data": dt.date.today().strftime("%d/%m/%Y"),
        "CPF/CNPJ Comprador": contact.get("doc", ""),
        "Endereço Comprador": contact.get("address", ""),
        "Número Comprador": contact.get("address_number", ""),
        "Complemento Comprador": contact.get("address_comp", ""),
        "Bairro Comprador": contact.get("address_district", ""),
        "CEP Comprador": contact.get("address_zip_code", ""),
        "Cidade Comprador": contact.get("address_city", ""),
        "UF Comprador": contact.get("address_state", ""),
        "Telefone Comprador": telefone,
        "Celular Comprador": telefone,
        "E-mail Comprador": contact.get("email", ""),
        # Entrega
        "Nome Entrega": contact.get("name", ""),
        "Endereço Entrega": contact.get("address", ""),
        "Número Entrega": contact.get("address_number", ""),
        "Complemento Entrega": contact.get("address_comp", ""),
        "Bairro Entrega": contact.get("address_district", ""),
        "CEP Entrega": contact.get("address_zip_code", ""),
        "Cidade Entrega": contact.get("address_city", ""),
        "UF Entrega": contact.get("address_state", ""),
        # Pedido
        "Un": "UN",
        "Quantidade": "1",
        "SKU": "",
        "subscription_id": subscription_id or "",
        "product_id": transacao.get("product", {}).get("internal_id", ""),
        "Plano Assinatura": tipo_plano or "",
        "periodicidade": valores.get("periodicidade", ""),
        "Cupom": cupom_valido,
        # Extras padrão
        "Número pedido": "",
        "Total Pedido": "",
        "Valor Frete Pedido": "",
        "Valor Desconto Pedido": "",
        "Outras despesas": "",
        "Transportadora": "",
        "Serviço": "",
        "Tipo Frete": "",
        "Observações": "",
        "Qtd Parcela": "",
        "Data Prevista": "",
        "Vendedor": "",
        "Forma Pagamento": valores.get("forma_pagamento", ""),
        "ID Forma Pagamento": "",
        "transaction_id": valores["transaction_id"],
        "indisponivel": "",
    }

class MapPedido(TypedDict):
    transaction_id: str
    id_oferta: str
    produto_principal: str
    sku_principal: str
    peso_principal: float | int
    valor_unitario: float
    valor_total: float
    total_pedido: float
    valor_embutido: float
    incluir_embutido: bool
    embutido: str
    brindes_extras: Sequence[dict[str, Any]]
    data_pedido: dt.datetime
    forma_pagamento: str
    usou_cupom: bool
    tipo_plano: str
    periodicidade: str
    divisor: int

class SKUInfo(TypedDict, total=False):
    sku: str
    peso: float | int
    periodicidade: str
    guru_ids: Sequence[str]

SKUInfo = Mapping[str, Any]

SKUs = Mapping[str, SKUInfo]

class AplicarRegrasAssinaturas(TypedDict, total=False):
    override_box: str | None
    brindes_extra: list[dict[str, Any]]

def _norm(s: str) -> str:
    return unidecode((s or "").strip().lower())

def formatar_valor(valor: float) -> str:
    return f"{valor:.2f}".replace(".", ",")

class RegrasConfig(TypedDict):
    rules: list[dict[str, Any]]

def _normalizar_cfg(cfg: Mapping[str, Any]) -> RegrasConfig:
    cfg = dict(cfg or {})  # tolera objetos Mapping
    rules = cfg.get("rules")
    if rules is None:
        rules = cfg.get("regras")  # legado
    if not isinstance(rules, list):
        rules = []
    return {"rules": cast(list[dict[str, Any]], rules)}

def obter_regras_config(path: str | None = None) -> list[dict[str, Any]]:
    path = path or _caminho_config_ofertas()
    try:
        with open(path, encoding="utf-8") as f:
            cfg: dict[str, Any] = json.load(f)
    except FileNotFoundError:
        print(f"[⚠️] {path} não encontrado")
        return []
    except Exception as e:
        print(f"[⚠️ ERRO ao ler {path}]: {e}")
        return []
    return cast(list[dict[str, Any]], _normalizar_cfg(cfg)["rules"])

def _caminho_config_ofertas() -> str:
    # tenta na raiz do projeto
    raiz = Path(__file__).resolve().parents[2]
    path_raiz = raiz / "config_ofertas.json"
    if path_raiz.exists():
        return str(path_raiz)
    # fallback: ao lado do módulo (comportamento antigo)
    return os.path.join(os.path.dirname(__file__), "config_ofertas.json")

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

def calcular_valores_pedidos(
    transacao: Mapping[str, Any],
    dados: Mapping[str, Any],
    skus_info: SKUs,
    usar_valor_fixo: bool = False,
) -> MapPedido:
    def _to_ts(val: Any) -> float | None:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            v = float(val)
            if v > 1e12:  # ms -> s
                v /= 1000.0
            return v
        if isinstance(val, dt.datetime):
            dtx = val if val.tzinfo else val.replace(tzinfo=UTC)
            return dtx.timestamp()
        if hasattr(val, "toPyDateTime"):
            try:
                dtx = val.toPyDateTime()
                dtx = dtx if dtx.tzinfo else dtx.replace(tzinfo=UTC)
                return dtx.timestamp()
            except Exception:
                return None
        if isinstance(val, str):
            try:
                dtx = parse_date(val)
                dtx = dtx if getattr(dtx, "tzinfo", None) else dtx.replace(tzinfo=UTC)
                return dtx.timestamp()
            except Exception:
                return None
        return None

    modo: str = str(dados.get("modo") or "").strip().lower()

    transaction_id: str = str(transacao.get("id", ""))
    product: Mapping[str, Any] = cast(Mapping[str, Any], transacao.get("product") or {})
    internal_id: str = str(product.get("internal_id") or "").strip()
    offer: Mapping[str, Any] = cast(Mapping[str, Any], product.get("offer") or {})
    id_oferta: str = str(offer.get("id", ""))

    print(f"[DEBUG calcular_valores_pedidos] id={transaction_id} internal_id={internal_id} modo={modo}")

    invoice: Mapping[str, Any] = cast(Mapping[str, Any], transacao.get("invoice") or {})
    is_upgrade: bool = invoice.get("type") == "upgrade"

    # 🔐 data_pedido robusta (timestamp seg/ms ou ISO; normaliza para naive)
    ts = (cast(Mapping[str, Any], transacao.get("dates") or {})).get("ordered_at")
    if ts is not None:
        try:
            val_f = float(ts)
            if val_f > 1e12:  # ms → s
                val_f /= 1000.0
            data_pedido: dt.datetime = dt.datetime.fromtimestamp(val_f, tz=UTC)
        except Exception:
            s = str(transacao.get("ordered_at") or transacao.get("created_at") or "1970-01-01")
            dtp = parse_date(s)
            data_pedido = dtp.replace(tzinfo=None) if getattr(dtp, "tzinfo", None) else dtp
    else:
        s = str(transacao.get("ordered_at") or transacao.get("created_at") or "1970-01-01")
        dtp = parse_date(s)
        data_pedido = dtp.replace(tzinfo=None) if getattr(dtp, "tzinfo", None) else dtp

    payment: Mapping[str, Any] = cast(Mapping[str, Any], transacao.get("payment") or {})
    try:
        valor_total_pago: float = float(payment.get("total") or 0)
    except Exception:
        valor_total_pago = 0.0

    coupon_info_raw: Any = payment.get("coupon", {})
    coupon_info: Mapping[str, Any] = coupon_info_raw if isinstance(coupon_info_raw, dict) else {}
    cupom: str = str(coupon_info.get("coupon_code") or "").strip().lower()
    incidence_type: str = str(coupon_info.get("incidence_type") or "").strip().lower()

    # 🔎 produto principal (via internal_id → skus_info) com fallbacks
    produto_principal: str | None = None
    if internal_id:
        for nome, info in skus_info.items():
            try:
                if internal_id in (info.get("guru_ids") or []):
                    produto_principal = nome
                    break
            except Exception:
                pass

    if not produto_principal:
        nome_prod_api = str(product.get("name") or "").strip()
        if nome_prod_api in skus_info:
            produto_principal = nome_prod_api

    if not produto_principal:
        nome_box = str(dados.get("box_nome") or "").strip()
        if nome_box:
            produto_principal = nome_box

    if not produto_principal:
        try:
            produto_principal = next(iter(skus_info.keys()))
            print(
                f"[⚠️ calcular_valores_pedidos] internal_id '{internal_id}' sem match; usando fallback '{produto_principal}'."
            )
        except StopIteration:
            print(f"[⚠️ calcular_valores_pedidos] skus_info vazio; retornando estrutura mínima para '{transaction_id}'.")
            return MapPedido(
                transaction_id=transaction_id,
                id_oferta=id_oferta,
                produto_principal="",
                sku_principal="",
                peso_principal=0,
                valor_unitario=round(valor_total_pago, 2),
                valor_total=round(valor_total_pago, 2),
                total_pedido=round(valor_total_pago, 2),
                valor_embutido=0.0,
                incluir_embutido=False,
                embutido="",
                brindes_extras=[],
                data_pedido=data_pedido,
                forma_pagamento=str(payment.get("method", "") or ""),
                usou_cupom=bool(cupom),
                tipo_plano="",
                periodicidade="",
                divisor=1,
            )

    info_produto: SKUInfo = skus_info.get(produto_principal, {}) or {}
    sku_principal: str = str(info_produto.get("sku", "") or "")
    peso_principal: float | int = cast(float | int, info_produto.get("peso", 0))

    # 🚫 Sem regras para 'produtos' OU quando não tiver assinatura
    if modo == "produtos" or not transacao.get("subscription"):
        return MapPedido(
            transaction_id=transaction_id,
            id_oferta=id_oferta,
            produto_principal=produto_principal,
            sku_principal=sku_principal,
            peso_principal=peso_principal,
            valor_unitario=round(valor_total_pago, 2),
            valor_total=round(valor_total_pago, 2),
            total_pedido=round(valor_total_pago, 2),
            valor_embutido=0.0,
            incluir_embutido=False,
            embutido="",
            brindes_extras=[],
            data_pedido=data_pedido,
            forma_pagamento=str(payment.get("method", "") or ""),
            usou_cupom=bool(cupom),
            tipo_plano="",
            periodicidade="",
            divisor=1,
        )

    # =========================
    # ASSINATURAS
    # =========================
    # ✅ janela/regras protegidas
    try:
        print(f"[DEBUG janela-check] id={transaction_id} data_pedido={data_pedido}")
        aplica_regras_neste_periodo: bool = bool(
            validar_regras_assinatura(
                cast(dict[Any, Any], dados),  # <-- converte Mapping -> dict p/ mypy
                data_pedido,
            )
        )
    except Exception as e:
        print(f"[DEBUG janela-skip] Erro em validar_regras_assinatura: {e}")
        aplica_regras_neste_periodo = False

    # Regras/cupom/override só se dentro do período
    if aplica_regras_neste_periodo:
        try:
            regras_aplicadas: AplicarRegrasAssinaturas = cast(
                AplicarRegrasAssinaturas,
                aplicar_regras_assinaturas(
                    cast(dict[Any, Any], transacao),  # <-- Mapping -> dict
                    cast(dict[Any, Any], dados),  # <-- Mapping -> dict
                    cast(dict[Any, Any], skus_info),  # <-- Mapping[str, SKUInfo] -> dict[Any, Any]
                    produto_principal,
                )
                or {},
            )
        except Exception as e:
            print(f"[⚠️ regras] Erro em aplicar_regras_assinaturas: {e}")
            regras_aplicadas = AplicarRegrasAssinaturas()
    else:
        regras_aplicadas = AplicarRegrasAssinaturas()

    override_box: str | None = cast(str | None, regras_aplicadas.get("override_box"))
    brindes_extra_por_regra: Sequence[dict[str, Any]] = regras_aplicadas.get("brindes_extra", []) or []

    if override_box:
        produto_principal = override_box
        info_produto = skus_info.get(produto_principal, {}) or {}
        sku_principal = str(info_produto.get("sku", "") or "")
        peso_principal = cast(float | int, info_produto.get("peso", 0))

    tipo_assinatura: str = str(transacao.get("tipo_assinatura", "") or "")

    # Cupons personalizados só se dentro do período
    if aplica_regras_neste_periodo:
        if tipo_assinatura in ("anuais", "bianuais", "trianuais"):
            # novos nomes → fallback para os antigos
            mapa = cast(Mapping[str, Any], dados.get("cupons_personalizados_cdf")
                        or dados.get("cupons_personalizados_anual") or {})
            prod_custom = mapa.get(cupom)
        elif tipo_assinatura in ("bimestrais", "mensais"):
            mapa = cast(Mapping[str, Any], dados.get("cupons_personalizados_bi_mens")
                        or dados.get("cupons_personalizados_bimestral") or {})
            prod_custom = mapa.get(cupom)
        else:
            prod_custom = None

        if prod_custom and prod_custom in skus_info:
            produto_principal = cast(str, prod_custom)
            info_produto = skus_info.get(produto_principal, {}) or {}
            sku_principal = str(info_produto.get("sku", "") or "")
            peso_principal = cast(float | int, info_produto.get("peso", 0))

    # periodicidade: override manual → produto → inferência
    periodicidade: str = (
        str(
            dados.get("periodicidade_selecionada")
            or dados.get("periodicidade")
            or info_produto.get("periodicidade")
            or ("mensal" if tipo_assinatura == "mensais" else "bimestral")
            or ""
        )
        .strip()
        .lower()
    )

    # embutido via oferta (respeita timestamps E a janela)
    ofertas_embutidas = cast(Mapping[str, Any], dados.get("ofertas_embutidas") or {})
    nome_embutido: str = str(ofertas_embutidas.get(str(id_oferta).strip(), "") or "")

    ini_ts = _to_ts(dados.get("embutido_ini_ts"))
    end_ts = _to_ts(dados.get("embutido_end_ts"))
    dp_ts = _to_ts(data_pedido)

    incluir_embutido: bool = bool(
        nome_embutido
        and dp_ts is not None
        and ini_ts is not None
        and end_ts is not None
        and ini_ts <= dp_ts <= end_ts
        and aplica_regras_neste_periodo
    )
    valor_embutido: float = 0.0

    # 💰 tabela para assinaturas multi-ano
    tabela_valores: Mapping[tuple[str, str], float] = {
        ("anuais", "mensal"): 960,
        ("anuais", "bimestral"): 480,
        ("bianuais", "mensal"): 1920,
        ("bianuais", "bimestral"): 960,
        ("trianuais", "mensal"): 2880,
        ("trianuais", "bimestral"): 1440,
    }

    # Cálculo do valor da assinatura
    if is_upgrade or usar_valor_fixo:
        valor_assinatura = float(tabela_valores.get((tipo_assinatura, periodicidade), valor_total_pago))
        if incidence_type == "percent":
            try:
                desconto = float(coupon_info.get("incidence_value") or 0)
            except Exception:
                desconto = 0.0
            valor_assinatura = round(valor_assinatura * (1 - desconto / 100), 2)
        incluir_embutido = False
        valor_embutido = 0.0

    elif tipo_assinatura in ("anuais", "bianuais", "trianuais"):
        valor_assinatura = float(tabela_valores.get((tipo_assinatura, periodicidade), valor_total_pago))
        if incidence_type == "percent":
            try:
                desconto = float(coupon_info.get("incidence_value") or 0)
            except Exception:
                desconto = 0.0
            valor_assinatura = round(valor_assinatura * (1 - desconto / 100), 2)
        valor_embutido = max(0.0, round(valor_total_pago - valor_assinatura, 2))

    else:
        # Não é assinatura multi-ano → usa valor pago mesmo
        valor_assinatura = float(valor_total_pago)
        incluir_embutido = False
        valor_embutido = 0.0

    # divisor conforme período/periodicidade (com guarda)
    if tipo_assinatura == "trianuais":
        divisor = 36 if periodicidade == "mensal" else 18
    elif tipo_assinatura == "bianuais":
        divisor = 24 if periodicidade == "mensal" else 12
    elif tipo_assinatura == "anuais":
        divisor = 12 if periodicidade == "mensal" else 6
    elif tipo_assinatura == "bimestrais":
        divisor = 2 if periodicidade == "mensal" else 1
    elif tipo_assinatura == "mensais":
        divisor = 1
    else:
        divisor = 1

    divisor = max(int(divisor or 1), 1)
    valor_unitario: float = round(valor_assinatura / divisor, 2)
    valor_total: float = valor_unitario
    total_pedido: float = round(valor_unitario + (valor_embutido if incluir_embutido else 0.0), 2)

    return MapPedido(
        transaction_id=transaction_id,
        id_oferta=id_oferta,
        produto_principal=produto_principal,
        sku_principal=sku_principal,
        peso_principal=peso_principal,
        valor_unitario=valor_unitario,
        valor_total=valor_total,
        total_pedido=total_pedido,
        valor_embutido=valor_embutido,
        incluir_embutido=incluir_embutido,
        embutido=nome_embutido,
        brindes_extras=brindes_extra_por_regra,
        data_pedido=data_pedido,
        forma_pagamento=str(payment.get("method", "") or ""),
        usou_cupom=bool(cupom),
        tipo_plano=tipo_assinatura,
        periodicidade=periodicidade,
        divisor=divisor,
    )

def validar_regras_assinatura(dados: dict, data_pedido: dt.datetime) -> bool:
    """
    Retorna True se a data do pedido estiver dentro do período da assinatura,
    permitindo aplicar regras de ofertas/cupons configuradas.

    - NÃO aplica para modo 'produtos'.
    - Usa ordered_at_ini_periodo/ordered_at_end_periodo se existirem; senão, deriva via calcular_periodo_assinatura.
    - Converte TUDO para dt.datetime *aware* (UTC) antes de comparar.
    - Logs defensivos sem referenciar variáveis ainda não definidas.
    """

    def _aware_utc(dt: dt.datetime | None) -> dt.datetime | None:
        if dt is None:
            return None
        # Se vier naive, marca como UTC; se vier com tz, converte para UTC
        return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)

    def _to_dt(val: object) -> dt.datetime | None:
        """Converte val -> dt.datetime (UTC aware).

        Aceita dt.datetime/ISO/timestamp s|ms/QDateTime.
        """
        if val is None:
            return None
        if isinstance(val, dt.datetime):
            return _aware_utc(val)
        if isinstance(val, int | float):
            try:
                v = float(val)
                if v > 1e12:  # ms -> s
                    v /= 1000.0
                return dt.datetime.fromtimestamp(v, tz=UTC)
            except Exception:
                return None
        if isinstance(val, str):
            try:
                dtp = parse_date(val)  # mantém a função existente
                return _aware_utc(dtp)
            except Exception:
                return None
        if hasattr(val, "toPyDateTime"):
            try:
                return _aware_utc(val.toPyDateTime())
            except Exception:
                return None
        return None

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

def montar_planilha_vendas_guru(
    transacoes: Sequence[Mapping[str, Any] | Sequence[Mapping[str, Any]]],
    dados: Mapping[str, Any],
    atualizar_etapa: Callable[[str, int, int], Any] | None,
    skus_info: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]]]:

    df_planilha_parcial = pd.DataFrame()
    mapa_transaction_id_por_linha: dict[int, str] = {}
    brindes_indisp_set: set[str] = set()
    embutidos_indisp_set: set[str] = set()
    boxes_indisp_set: set[str] = set()

    # contagem por tipo (apenas assinaturas)
    tipos = ["anuais", "bimestrais", "bianuais", "trianuais", "mensais"]
    contagem: dict[str, dict[str, int]] = {tipo: {"assinaturas": 0, "embutidos": 0, "cupons": 0} for tipo in tipos}

    linhas_planilha: list[dict[str, Any]] = []
    offset = len(df_planilha_parcial)

    def _ckey(tp: str) -> str:
        t = (tp or "").strip().lower()
        if t in contagem:
            return t
        aliases = {"anual": "anuais", "bianual": "bianuais", "trianual": "trianuais", "bimestral": "bimestrais", "mensal": "mensais"}
        return aliases.get(t, "bimestrais")

    def _append_linha(linha: dict[str, Any], transaction_id: str) -> None:
        linhas_planilha.append(linha)
        mapa_transaction_id_por_linha[offset + len(linhas_planilha) - 1] = transaction_id

    def _flag_indisp(nome: str, sku: str | None = None) -> str:
        try:
            return "S" if produto_indisponivel(nome, sku=sku) else ""
        except Exception:
            return ""

    def _aplica_janela(dados_local: Mapping[str, Any], dt: dt.datetime) -> bool:
        try:
            return bool(validar_regras_assinatura(cast(dict[Any, Any], dados_local), dt))
        except Exception as e:
            print(f"[DEBUG janela-skip] Ignorando janela por falta de contexto: {e}")
            return False

    def _to_ts(val: Any) -> float | None:
        if val is None:
            return None
        if isinstance(val, (int, float)):
            v = float(val)
            if v > 1e12:  # ms -> s
                v /= 1000.0
            return v
        if isinstance(val, dt.datetime):
            dtx = val if val.tzinfo else val.replace(tzinfo=UTC)
            return dtx.timestamp()
        if hasattr(val, "toPyDateTime"):
            try:
                dtx = val.toPyDateTime()
                dtx = dtx if dtx.tzinfo else dtx.replace(tzinfo=UTC)
                return dtx.timestamp()
            except Exception:
                return None
        if isinstance(val, str):
            try:
                dtx = parse_date(val)
                dtx = dtx if getattr(dtx, "tzinfo", None) else dtx.replace(tzinfo=UTC)
                return dtx.timestamp()
            except Exception:
                return None
        return None

    # flatten defensivo
    transacoes_corrigidas: list[Mapping[str, Any]] = []
    for idx, t in enumerate(transacoes):
        if isinstance(t, Mapping):
            transacoes_corrigidas.append(t)
        elif isinstance(t, Sequence):
            print(f"[⚠️ montar_planilha_vendas_guru] Corrigindo lista aninhada em transacoes[{idx}]")
            for sub in t:
                if isinstance(sub, Mapping):
                    transacoes_corrigidas.append(sub)
                else:
                    print(f"[⚠️ Ignorado] Item inesperado do tipo {type(sub)} dentro de transacoes[{idx}]")
        else:
            print(f"[⚠️ Ignorado] transacoes[{idx}] é do tipo {type(t)} e será ignorado")

    transacoes = transacoes_corrigidas
    total_transacoes = len(transacoes)

    ids_planos_validos: Sequence[str] = cast(Sequence[str], dados.get("ids_planos_todos", []))
    ofertas_embutidas = dados.get("ofertas_embutidas", {}) or {}
    modo_periodo_sel = (dados.get("modo_periodo") or "").strip().upper()

    # ======== SOMENTE ASSINATURAS ========
    transacoes_por_assinatura: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for trans in transacoes:
        subscription_info = trans.get("subscription")
        if isinstance(subscription_info, Mapping):
            sid = subscription_info.get("id")
            if sid:
                transacoes_por_assinatura[str(sid)].append(trans)

    total_assinaturas = len(transacoes_por_assinatura)

    def is_transacao_principal(trans: Mapping[str, Any], ids_validos: Sequence[str]) -> bool:
        pid = trans.get("product", {}).get("internal_id", "")
        is_bump = bool(trans.get("is_order_bump", 0))
        return pid in ids_validos and not is_bump

    for i, (subscription_id, grupo_transacoes) in enumerate(transacoes_por_assinatura.items()):
        def safe_parse_date(t: Mapping[str, Any]) -> dt.datetime:
            try:
                s = str(t.get("ordered_at") or t.get("created_at") or "1900-01-01")
                dtp = parse_date(s)
                return dtp.astimezone(UTC) if dtp.tzinfo else dtp.replace(tzinfo=UTC)
            except Exception:
                return dt.datetime(1900, 1, 1, tzinfo=UTC)

        grupo_ordenado = sorted(grupo_transacoes, key=safe_parse_date)
        transacao_base = grupo_ordenado[-1]
        tipo_plano = str(transacao_base.get("tipo_assinatura", "bimestrais"))

        transacoes_principais = [t for t in grupo_ordenado if is_transacao_principal(t, ids_planos_validos)]
        produtos_distintos = {t.get("product", {}).get("internal_id") for t in transacoes_principais}

        usar_valor_fixo = len(produtos_distintos) > 1 or transacao_base.get("invoice", {}).get("type") == "upgrade"

        if not transacoes_principais:
            print(f"[⚠️ AVISO] Nenhuma transação principal encontrada para assinatura {subscription_id}")

        if usar_valor_fixo:
            valor_total_principal = 0.0
        elif transacoes_principais:
            valor_total_principal = sum(float(t.get("payment", {}).get("total", 0)) for t in transacoes_principais)
        else:
            valor_total_principal = float(transacao_base.get("payment", {}).get("total", 0))

        # transação “sintética”
        transacao = dict(transacao_base)
        transacao.setdefault("payment", {})
        transacao["payment"]["total"] = valor_total_principal
        transacao["tipo_assinatura"] = tipo_plano
        transacao["subscription"] = {"id": subscription_id}

        product_base = cast(Mapping[str, Any], transacao_base.get("product", cast(Mapping[str, Any], {})))
        transacao.setdefault("product", {})
        if "offer" not in transacao["product"] and product_base.get("offer"):
            transacao["product"]["offer"] = product_base["offer"]

        try:
            valores = calcular_valores_pedidos(
                transacao,
                dados,
                cast(Mapping[str, SKUInfo], skus_info),
                usar_valor_fixo=usar_valor_fixo,
            )
            if not isinstance(valores, Mapping) or not valores.get("transaction_id"):
                raise ValueError(f"Valores inválidos retornados: {valores}")

            periodicidade_atual = (
                dados.get("periodicidade_selecionada")
                or dados.get("periodicidade")
                or valores.get("periodicidade")
                or ""
            )
            periodicidade_atual = str(periodicidade_atual).strip().lower()

            data_fim_periodo = dados.get("ordered_at_end_periodo")
            data_pedido: dt.datetime = cast(dt.datetime, valores["data_pedido"])

            # cupom
            payment_base = transacao_base.get("payment") or {}
            coupon = payment_base.get("coupon") or {}
            cupom_usado = (coupon.get("coupon_code") or "").strip()
            if valores.get("usou_cupom"):
                contagem[_ckey(tipo_plano)]["cupons"] += 1

            # linha base
            contact = transacao.get("contact", {})
            linha = gerar_linha_base_planilha(
                contact,
                valores,
                transacao,
                tipo_plano=tipo_plano,
                subscription_id=subscription_id,
                cupom_valido=cupom_usado,
            )

            nome_produto_principal = (dados.get("box_nome") or "").strip() or str(valores["produto_principal"])
            if produto_indisponivel(nome_produto_principal):
                boxes_indisp_set.add(nome_produto_principal)

            linha["Produto"] = nome_produto_principal
            linha["SKU"] = skus_info.get(nome_produto_principal, {}).get("sku", "")
            linha["Valor Unitário"] = formatar_valor(valores["valor_unitario"])
            linha["Valor Total"] = formatar_valor(valores["valor_total"])
            linha["periodicidade"] = periodicidade_atual
            linha["indisponivel"] = _flag_indisp(
                nome_produto_principal, skus_info.get(nome_produto_principal, {}).get("sku", "")
            )

            # período (mês/bimestre)
            def _calc_periodo(periodicidade: str, data_ref: dt.datetime) -> int | str:
                if periodicidade == "mensal":
                    return data_ref.month
                elif periodicidade == "bimestral":
                    return 1 + ((data_ref.month - 1) // 2)
                return ""

            if modo_periodo_sel == "TODAS":
                linha["periodo"] = _calc_periodo(periodicidade_atual, data_pedido)
            elif dados.get("periodo"):
                linha["periodo"] = dados["periodo"]
            else:
                mes_ref = data_fim_periodo if isinstance(data_fim_periodo, dt.datetime) else data_pedido
                linha["periodo"] = _calc_periodo(periodicidade_atual, mes_ref)

            _append_linha(linha, str(valores["transaction_id"]))

            # janela obrigatória para brindes
            if not _aplica_janela(dados, data_pedido):
                valores["brindes_extras"] = []

            # brindes extras (somente na janela)
            for br in valores.get("brindes_extras") or []:
                brinde_nome = str(br.get("nome", "")).strip() if isinstance(br, Mapping) else str(br).strip()
                if not brinde_nome:
                    continue
                sku_b = skus_info.get(brinde_nome, {}).get("sku", "")
                lb = dict(linha)
                lb.update(
                    {
                        "Produto": brinde_nome,
                        "SKU": sku_b,
                        "Valor Unitário": "0,00",
                        "Valor Total": "0,00",
                        "indisponivel": _flag_indisp(brinde_nome, sku_b),
                        "subscription_id": subscription_id,
                    }
                )
                if lb["indisponivel"] == "S":
                    brindes_indisp_set.add(brinde_nome)
                _append_linha(lb, str(valores["transaction_id"]))

            # embutidos por oferta (validados + dentro da janela)
            oferta_id = transacao.get("product", {}).get("offer", {}).get("id")
            oferta_id_clean = str(oferta_id).strip()
            ofertas_normalizadas = {str(k).strip(): v for k, v in ofertas_embutidas.items()}
            nome_embutido_oferta = str(ofertas_normalizadas.get(oferta_id_clean) or "")

            data_pedido_ts = _to_ts(data_pedido)
            ini_ts = _to_ts(dados.get("embutido_ini_ts"))
            end_ts = _to_ts(dados.get("embutido_end_ts"))

            if (
                nome_embutido_oferta
                and data_pedido_ts is not None
                and ini_ts is not None
                and end_ts is not None
                and ini_ts <= data_pedido_ts <= end_ts
                and _aplica_janela(dados, data_pedido)
            ):
                sku_emb = skus_info.get(nome_embutido_oferta, {}).get("sku", "")
                le = dict(linha)
                le.update(
                    {
                        "Produto": nome_embutido_oferta,
                        "SKU": sku_emb,
                        "Valor Unitário": "0,00",
                        "Valor Total": "0,00",
                        "indisponivel": _flag_indisp(nome_embutido_oferta, sku_emb),
                        "subscription_id": subscription_id,
                    }
                )
                if le["indisponivel"] == "S":
                    embutidos_indisp_set.add(nome_embutido_oferta)
                _append_linha(le, str(valores["transaction_id"]))
                contagem[_ckey(tipo_plano)]["embutidos"] += 1

            contagem[_ckey(tipo_plano)]["assinaturas"] += 1

        except Exception as e:
            print(f"[❌ ERRO] Transação {transacao.get('id')}: {e}")
            traceback.print_exc()

        try:
            if callable(atualizar_etapa):
                atualizar_etapa("📦 Processando transações", i + 1, total_assinaturas or 1)
        except Exception as e:
            print(f"[❌ ERRO ao atualizar progresso]: {e}")
            traceback.print_exc()

    # ===== saída/merge =====
    try:
        df_novas = pd.DataFrame(linhas_planilha)
    except Exception as e:
        print(f"[DEBUG df_error] {type(e).__name__}: {e}")
        if linhas_planilha:
            print(f"[DEBUG ultima_linha] keys={list(linhas_planilha[-1].keys())}")
        raise

    df_novas = padronizar_planilha_bling(df_novas)
    if "indisponivel" in df_novas.columns:
        df_novas["indisponivel"] = df_novas["indisponivel"].map(
            lambda x: "S" if str(x).strip().lower() in {"s", "sim", "true", "1"} else ""
        )
    else:
        df_novas["indisponivel"] = [""] * len(df_novas)

    if not df_planilha_parcial.empty:
        df_planilha_parcial = pd.concat([df_planilha_parcial, df_novas], ignore_index=True)
    else:
        df_planilha_parcial = df_novas

    if callable(atualizar_etapa):
        atualizar_etapa("✅ Processamento concluído", total_transacoes, total_transacoes or 1)

    return linhas_planilha, contagem


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
