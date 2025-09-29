import datetime as dt
import traceback
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypedDict, cast

import pandas as pd
from app.services.coleta_vendas_assinaturas import AplicarRegrasAssinaturas, aplicar_regras_assinaturas, validar_regras_assinatura
from dateutil.parser import parse as parse_date
from mapeamento import SKUInfo, SKUs, produto_indisponivel

UTC = dt.UTC


def formatar_valor(valor: float) -> str:
    return f"{valor:.2f}".replace(".", ",")


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
        aliases = {
            "anual": "anuais",
            "bianual": "bianuais",
            "trianual": "trianuais",
            "bimestral": "bimestrais",
            "mensal": "mensais",
        }
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
            mapa = cast(
                Mapping[str, Any],
                dados.get("cupons_personalizados_cdf") or dados.get("cupons_personalizados_anual") or {},
            )
            prod_custom = mapa.get(cupom)
        elif tipo_assinatura in ("bimestrais", "mensais"):
            mapa = cast(
                Mapping[str, Any],
                dados.get("cupons_personalizados_bi_mens") or dados.get("cupons_personalizados_bimestral") or {},
            )
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
