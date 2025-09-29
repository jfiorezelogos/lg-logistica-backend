# sitecustomize.py — inicializa logging e correlation id automaticamente
from __future__ import annotations

import logging
import os

from app.common.logging_setup import (  # ajuste o path conforme seu projeto
    redirect_std_streams_to_logger,
    set_correlation_id,
    setup_logging,
)

# 1) configura logging global (respeita LOG_LEVEL, LOG_JSON, LOG_FILE)
level = logging.DEBUG if os.getenv("DEBUG") == "1" else logging.INFO
log_file = os.path.join(os.getcwd(), "sistema.log")
setup_logging(level=level, json_console=True, file_path=log_file)

# 2) gera um id único por execução (aparece como correlation_id nos logs)
set_correlation_id()

bootstrap_log = logging.getLogger("bootstrap")
bootstrap_log.info("logging inicializado via sitecustomize.py")

# 3) (opcional) capturar print()/stderr e mandar para o logger
redirect_std_streams_to_logger(env_switch="LOG_CAPTURE_STDOUT")
