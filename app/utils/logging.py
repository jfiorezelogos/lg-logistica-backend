# app/utils/logging.py
from __future__ import annotations

import warnings

# Re-exporta do ponto único de verdade
from app.common.logging_setup import (  # ajuste o path conforme seu projeto
    correlation_id_ctx,
    get_correlation_id,
    set_correlation_id,
)

# (opcional) alerta de depreciação suave para quem importar este módulo diretamente
warnings.warn(
    "app.services.logging_utils foi unificado: use app.common.logging_setup "
    "para correlation_id e helpers de logging.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["correlation_id_ctx", "set_correlation_id", "get_correlation_id"]