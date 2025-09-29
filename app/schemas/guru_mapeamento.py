from typing import Literal

from pydantic import BaseModel


class MapearGuruRequest(BaseModel):
    sku: str
    tipo: Literal["produto", "assinatura", "combo"]
    guru_ids: list[str]
    recorrencia: str | None
    periodicidade: str | None

    class Config:
        json_schema_extra = {
            "example": {
                "sku": "ASS01A-MES",
                "tipo": "assinatura",
                "guru_ids": ["9d5d8fe8-2000-4716-a609-5619d8f91c87", "9dc506f6-bf75-444b-a347-38c81026af53"],
                "recorrencia": "anual",
                "periodicidade": "mensal",
            }
        }


class MapearGuruResponse(BaseModel):
    sku: str
    tipo: str
    guru_ids: list[str]
    recorrencia: str | None
    periodicidade: str | None
    message: str
