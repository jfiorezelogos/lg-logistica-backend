# app/main.py
from __future__ import annotations

import logging
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.common.middlewares import RequestIdMiddleware
from app.routers.catalogo import router as catalogo_router
from app.routers.guru_importar_planilha import router as guru_importar_planilha_router
from app.routers.guru_produtos import router as guru_produtos_router
from app.routers.guru_regras import router as regras_router
from app.routers.guru_vendas_assinaturas import router as guru_vendas_assinaturas_router
from app.routers.guru_vendas_produtos import router as guru_vendas_produtos_router
from app.routers.shopify_produtos import router as shopify_produtos_router
from app.routers.shopify_vendas_produtos import router as shopify_vendas_router

# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),                    # console
        logging.FileHandler(LOG_DIR / "app.log"),   # arquivo
    ],
)
logger = logging.getLogger(__name__)
logger.info("Inicializando LG Logística v2...")

# ----------------------------------------------------------------------
# FastAPI app
# ----------------------------------------------------------------------
app = FastAPI(title="API LG Logística v2")

# Middleware de request-id
app.add_middleware(RequestIdMiddleware)

# ✅ CORS — em produção restrinja a origens confiáveis
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ registra routers
app.include_router(guru_vendas_assinaturas_router)
app.include_router(guru_vendas_produtos_router)
app.include_router(shopify_vendas_router)
app.include_router(guru_produtos_router)
app.include_router(shopify_produtos_router)
app.include_router(regras_router)
app.include_router(catalogo_router)
app.include_router(guru_importar_planilha_router)

# ✅ rota de saúde simples
@app.get("/health", tags=["Health"])
def health() -> dict[str, bool]:
    return {"ok": True}
