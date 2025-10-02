# app/common/middlewares.py
from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.common.logging_setup import get_correlation_id, set_correlation_id


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        # Reaproveita X-Request-Id se cliente enviar, senão gera UUID novo
        cid = request.headers.get("X-Request-Id") or str(uuid.uuid4())
        set_correlation_id(cid)

        # Executa a request
        response = await call_next(request)

        # Sempre devolve o correlation id no header
        response.headers["X-Request-Id"] = get_correlation_id()
        return response
