# app/main.py
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.common.middlewares import RequestIdMiddleware
from app.routers.catalogo import router as catalogo_router
from app.routers.guru_importar_planilha import router as importar_planilha_router
from app.routers.guru_produtos import router as guru_produtos_router
from app.routers.guru_regras import router as regras_router
from app.routers.guru_vendas_assinaturas import router as vendas_assinaturas_router
from app.routers.guru_vendas_produtos import router as vendas_produtos_router
from app.routers.shopify_produtos import router as shopify_produtos_router

app = FastAPI(title="LG Logística v2 - Backend")

# Logging
app.add_middleware(RequestIdMiddleware)

# ✅ CORS — importante para Swagger e integrações com front-ends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # em produção restrinja: ["http://localhost:3000", "https://meusite.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ registra routers
app.include_router(vendas_assinaturas_router)
app.include_router(vendas_produtos_router)
app.include_router(guru_produtos_router)
app.include_router(shopify_produtos_router)
app.include_router(regras_router)
app.include_router(catalogo_router)
app.include_router(importar_planilha_router)


# ✅ rota de saúde simples
@app.get("/health", tags=["Health"])
def health() -> dict[str, bool]:
    return {"ok": True}
