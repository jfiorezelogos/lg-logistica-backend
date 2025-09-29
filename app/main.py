from __future__ import annotations

from fastapi import FastAPI

# importa os routers pelo nome real dos arquivos
from app.routers.coleta_vendas_assinaturas import router as assinaturas_router
from app.routers.coleta_vendas_produtos import router as produtos_router  # se já existir

app = FastAPI(title="LG Logística v2 - Backend")

# registra routers
app.include_router(assinaturas_router)
app.include_router(produtos_router)  # remova se ainda não criou a rota de produtos

# rota de saúde simples
@app.get("/health", tags=["Health"])
def health() -> dict[str, bool]:
    return {"ok": True}
