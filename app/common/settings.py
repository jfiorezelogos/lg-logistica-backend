# common/settings.py
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent  # sobe até a pasta do main.py


class Settings(BaseSettings):
    API_KEY_GURU: str = ""  # default evita erro no mypy
    SHOP_URL: str = ""
    SHOPIFY_TOKEN: str = ""
    OPENAI_API_KEY: str = ""
    FRETEBARATO_URL: str = ""
    APP_ENV: str = "dev"
    GURU_MAX_CONCURRENCY: int = 4  # quantas requisições simultâneas
    GURU_QPS: float = 3.0  # requisições por segundo (média)

    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),  # busca o .env na raiz do projeto
        env_file_encoding="utf-8",
        extra="ignore",
    )


# Em runtime, pydantic-settings vai sobrescrever com valores do .env/ambiente
settings: Settings = Settings()
