from __future__ import annotations

import uuid
from collections.abc import Awaitable, Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from app.common.logging_setup import set_correlation_id


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        # Reaproveita X-Request-ID se cliente enviar, senão gera UUID novo
        rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        set_correlation_id(rid)

        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response
