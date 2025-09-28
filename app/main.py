from fastapi import FastAPI
from app.routers.assinaturas import router as assinaturas_router

app = FastAPI(title="LG Logística v2 - Backend")
app.include_router(assinaturas_router)

@app.get("/health")
def health():
    return {"ok": True}
