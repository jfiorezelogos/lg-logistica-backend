# app/main.py
from __future__ import annotations

from fastapi import FastAPI

# importa routers
from app.routers.guru_vendas_assinaturas import router as assinaturas_router
from app.routers.guru_vendas_produtos import router as produtos_router
from app.routers.guru_regras import router as regras_router
from app.routers.catalogo import router as skus_router

app = FastAPI(title="LG Logística v2 - Backend")

# registra routers
app.include_router(assinaturas_router)
app.include_router(produtos_router)
app.include_router(regras_router)
app.include_router(skus_router)

# rota de saúde simples
@app.get("/health", tags=["Health"])
def health() -> dict[str, bool]:
    return {"ok": True}
