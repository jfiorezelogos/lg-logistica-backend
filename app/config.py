# app/config.py
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # valores padrão
    timezone: str = Field(
        default="America/Sao_Paulo",
        # nomes de env aceitos (primeiro que existir é usado)
        validation_alias=AliasChoices("TZ", "TIMEZONE"),
    )
    regras_path: str = Field(
        default="app/config_ofertas.json",
        validation_alias=AliasChoices("REGRAS_PATH"),
    )

    # configuração do carregamento
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def resolve_path(p: str) -> Path:
    path = Path(p)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path
