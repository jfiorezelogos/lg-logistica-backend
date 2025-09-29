from __future__ import annotations

from typing import Any


def normalizar_rules(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """Aceita 'rules' ou legado 'regras' e retorna lista."""
    regras = cfg.get("rules", cfg.get("regras"))
    return regras if isinstance(regras, list) else []


def montar_ofertas_embutidas(cfg: dict[str, Any]) -> dict[str, str]:
    """
    Constrói {oferta_id: nome_do_produto_embutido} a partir das regras de 'oferta'
    cujo action.type == 'adicionar_brindes'. Se houver lista de brindes, pega o primeiro.
    """
    mapa: dict[str, str] = {}
    for r in cfg.get("rules", []):
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
      - cupons_cdf:     {cupom_lower: box}  -> Anual, 2 anos/Bianual, 3 anos/Trianual
      - cupons_bi_mens: {cupom_lower: box}  -> Bimestral, Mensal
    Considera apenas regras de cupom com action.type == 'alterar_box'.
    """
    cupons_cdf: dict[str, str] = {}
    cupons_bi_mens: dict[str, str] = {}

    for r in cfg.get("rules", []):
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


__all__ = ["montar_mapas_cupons", "montar_ofertas_embutidas", "normalizar_rules"]
