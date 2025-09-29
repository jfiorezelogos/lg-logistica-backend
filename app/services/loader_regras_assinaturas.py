from __future__ import annotations

from typing import Any


# =========================
# Regras (cfg)
# =========================
def normalizar_rules(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Aceita 'rules' ou legado 'regras' e retorna lista."""
    regras = cfg.get("rules", cfg.get("regras"))
    return regras if isinstance(regras, list) else []


def montar_ofertas_embutidas(cfg: dict[str, Any]) -> dict[str, str]:
    """
    Gera {oferta_id: nome_do_produto_embutido} a partir de regras:
      applies_to='oferta' com action.type='adicionar_brindes'.
    """
    mapa: dict[str, str] = {}
    for r in normalizar_rules(cfg):
        if (r.get("applies_to") or "").strip().lower() != "oferta":
            continue
        action = r.get("action") or {}
        if (action.get("type") or "").strip().lower() != "adicionar_brindes":
            continue

        oferta = r.get("oferta") or {}
        oferta_id = str(oferta.get("oferta_id") or oferta.get("id") or "").strip()
        if not oferta_id:
            continue

        brindes = action.get("brindes") or []
        nome: str | None = None
        if isinstance(brindes, list) and brindes:
            b0 = brindes[0]
            if isinstance(b0, str):
                nome = b0.strip()
            elif isinstance(b0, dict):
                nome = str(b0.get("nome") or b0.get("name") or "").strip()
        if nome:
            mapa[oferta_id] = nome
    return mapa


def montar_mapas_cupons(cfg: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    """
    Retorna:
      - cupons_cdf:     {cupom_lower: box}  -> Anual / 2 anos / 3 anos
      - cupons_bi_mens: {cupom_lower: box}  -> Bimestral / Mensal
    Só considera regras applies_to='cupom' com action.type='alterar_box'.
    """
    cupons_cdf: dict[str, str] = {}
    cupons_bi_mens: dict[str, str] = {}

    for r in normalizar_rules(cfg):
        if (r.get("applies_to") or "").strip().lower() != "cupom":
            continue
        action = r.get("action") or {}
        if (action.get("type") or "").strip().lower() != "alterar_box":
            continue

        cupom = ((r.get("cupom") or {}).get("nome") or "").strip().lower()
        box = (action.get("box") or "").strip()
        if not cupom or not box:
            continue

        assinaturas = r.get("assinaturas") or []
        if isinstance(assinaturas, str):
            assinaturas = [assinaturas]
        txt = " | ".join(str(x) for x in assinaturas).lower()

        if any(k in txt for k in ("anual", "2 anos", "bianual", "3 anos", "trianual")):
            cupons_cdf[cupom] = box
        if any(k in txt for k in ("bimestral", "mensal")):
            cupons_bi_mens[cupom] = box

    return cupons_cdf, cupons_bi_mens


# =========================
# Assinaturas (helpers)
# =========================
TABELA_VALORES: dict[tuple[str, str], int] = {
    ("anuais", "mensal"): 960,
    ("anuais", "bimestral"): 480,
    ("bianuais", "mensal"): 1920,
    ("bianuais", "bimestral"): 960,
    ("trianuais", "mensal"): 2880,
    ("trianuais", "bimestral"): 1440,
}


def eh_assinatura(nome_produto: str) -> bool:
    return "assinatura" in (nome_produto or "").lower()


def inferir_periodicidade(id_produto: str) -> str:
    txt = (id_produto or "").upper()
    if "-MES" in txt:
        return "mensal"
    if "-BIM" in txt:
        return "bimestral"
    return "bimestral"


def inferir_tipo(nome_produto: str) -> str:
    s = (nome_produto or "").lower()
    if "3 anos" in s or "3 ano" in s or "3anos" in s:
        return "trianuais"
    if "2 anos" in s or "2 ano" in s or "2anos" in s:
        return "bianuais"
    if "anual" in s:
        return "anuais"
    if "bimestral" in s:
        return "bimestrais"
    if "mensal" in s:
        return "mensais"
    return "anuais"


def divisor_para(tipo: str, periodicidade: str) -> int:
    ta = (tipo or "").lower().strip()
    per = (periodicidade or "").lower().strip()
    if ta == "trianuais":
        return 36 if per == "mensal" else 18
    if ta == "bianuais":
        return 24 if per == "mensal" else 12
    if ta == "anuais":
        return 12 if per == "mensal" else 6
    if ta == "bimestrais":
        return 2 if per == "mensal" else 1
    if ta == "mensais":
        return 1
    return 1


__all__ = [
    "TABELA_VALORES",
    "divisor_para",
    "eh_assinatura",
    "inferir_periodicidade",
    "inferir_tipo",
    "montar_mapas_cupons",
    "montar_ofertas_embutidas",
    "normalizar_rules",
]
