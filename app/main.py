# app/main.py
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Logging unificado (JSON/UTC, mask de segredos, correlation id, captura de stdout/stderr)
from app.common.logging_setup import (
    get_logger,
    redirect_std_streams_to_logger,
    setup_logging,
)

# Middleware de correlação (garante X-Request-Id de entrada/saída)
# Se o seu módulo ainda exporta RequestIdMiddleware, troque o import abaixo.
from app.common.middlewares import CorrelationIdMiddleware
from app.routers.fretebarato_cotacao import router as cotar_fretes_router
from app.routers.guru_importar_planilha import router as guru_importar_planilha_router
from app.routers.guru_produtos import router as guru_produtos_router
from app.routers.guru_regras import router as regras_router
from app.routers.guru_vendas_assinaturas import router as guru_vendas_assinaturas_router
from app.routers.guru_vendas_produtos import router as guru_vendas_produtos_router

# Routers
from app.routers.produtos_catalogo import router as catalogo_router
from app.routers.shopify_fulfillment import router as shopify_fulfillment_router
from app.routers.shopify_produtos import router as shopify_produtos_router
from app.routers.shopify_vendas_produtos import router as shopify_vendas_router


# -----------------------------------------------------------------------------
# Inicialização de logging
# -----------------------------------------------------------------------------
def _init_logging() -> None:
    # Diretório de logs opcional (usado apenas se LOG_FILE apontar para dentro dele)
    Path("logs").mkdir(exist_ok=True)

    # Lê envs: LOG_LEVEL, LOG_JSON, LOG_FILE, LOG_MASK_SECRETS, APP_NAME, APP_VERSION, APP_ENV
    setup_logging()
    # Captura stdout/stderr (enable via env LOG_CAPTURE_STDOUT=1)
    redirect_std_streams_to_logger()
    # Opcional: fixa contexto do app (se quiser forçar env/correlation id inicial)
    # bind_context(app_env=os.getenv("APP_ENV", "dev"))

    logger = get_logger(__name__)
    logger.info(
        "app_startup",
        extra={"app": os.getenv("APP_NAME", "lg-logistica"), "version": os.getenv("APP_VERSION", "0.0.0")},
    )


# -----------------------------------------------------------------------------
# Criação do app
# -----------------------------------------------------------------------------
def create_app() -> FastAPI:
    _init_logging()

    app = FastAPI(title="API LG Logística v2")

    # Middleware de correlação (injeta/propaga X-Request-Id)
    app.add_middleware(CorrelationIdMiddleware)

    # CORS — em produção, restrinja as origens
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(guru_vendas_assinaturas_router)
    app.include_router(guru_vendas_produtos_router)
    app.include_router(shopify_vendas_router)
    app.include_router(guru_produtos_router)
    app.include_router(shopify_produtos_router)
    app.include_router(regras_router)
    app.include_router(catalogo_router)
    app.include_router(guru_importar_planilha_router)
    app.include_router(shopify_fulfillment_router)
    app.include_router(cotar_fretes_router)

    @app.get("/health", tags=["Health"])
    def health() -> dict[str, bool]:
        return {"ok": True}

    return app


# Instância utilizada pelo servidor (uvicorn/gunicorn)
app = create_app()
